package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/solana-foundation/solana-go/v2/rpc/jsonrpc"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
)

func TestAbort(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Parallel()

	cases := []struct {
		err    error
		status int
		code   string
	}{
		{api.ErrSessionNotFound, 404, "session_not_found"},
		{auth.ErrNonceReplayed, 401, "unauthorized"},
		{auth.ErrTicketExpired, 403, "invalid_ticket"},
		{&json.SyntaxError{}, 400, "bad_request"},
		{io.ErrUnexpectedEOF, 400, "bad_request"},
		{context.Canceled, 499, "client_closed_request"},
		{errors.New("boom"), 500, "internal"},
	}

	for _, tc := range cases {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		api.Abort(ctx, tc.err)
		require.Equal(t, tc.status, recorder.Code, tc.code)
		require.Contains(t, recorder.Body.String(), `"code":"`+tc.code+`"`)
	}
}

func TestAbort_TooManyRequestsCarriesRetryAfter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Parallel()

	busy := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(busy)
	api.Abort(ctx, fmt.Errorf("fetching channel: %w", api.ErrChainBusy))
	require.Equal(t, http.StatusTooManyRequests, busy.Code)
	require.Equal(t, "2", busy.Header().Get("Retry-After"))

	limited := httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(limited)
	ctx.Header("Retry-After", "7")
	api.Abort(ctx, api.ErrRateLimited)
	require.Equal(t, "7", limited.Header().Get("Retry-After"), "a limiter's own value is kept")

	notFound := httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(notFound)
	api.Abort(ctx, api.ErrSessionNotFound)
	require.Empty(t, notFound.Header().Get("Retry-After"))
}

func TestAbort_BusyChainReadAnywhereIsChainBusy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Parallel()

	signer := "H5j5QRrseSstL5JMJTXQU5vtQHMk2mS8wL1oCgLpwTMj"
	cases := []struct {
		err    error
		name   string
		status int
	}{
		{
			name:   "auth grant read rate limited",
			err:    fmt.Errorf("authorizing %s: checking grant: fetching grants: %w", signer, &jsonrpc.RPCError{Code: -32429}),
			status: http.StatusTooManyRequests,
		},
		{name: "json-rpc 429", err: &jsonrpc.RPCError{Code: http.StatusTooManyRequests}, status: http.StatusTooManyRequests},
		{name: "http 503", err: &jsonrpc.HTTPError{Code: http.StatusServiceUnavailable}, status: http.StatusTooManyRequests},
		{name: "json-rpc invalid params", err: &jsonrpc.RPCError{Code: -32602}, status: http.StatusInternalServerError},
		{name: "http 400", err: &jsonrpc.HTTPError{Code: http.StatusBadRequest}, status: http.StatusInternalServerError},
	}

	for _, tc := range cases {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		api.Abort(ctx, tc.err)
		require.Equal(t, tc.status, recorder.Code, tc.name)

		if tc.status == http.StatusTooManyRequests {
			require.Contains(t, recorder.Body.String(), `"code":"rate_limited"`, tc.name)
			require.NotEmpty(t, recorder.Header().Get("Retry-After"), tc.name)
		}
	}
}

func TestAbort_NilErrorRespondsInternal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Parallel()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	require.NotPanics(t, func() { api.Abort(ctx, nil) })
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestAbort_RecordsInternalCause(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Parallel()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	api.Abort(ctx, errors.New("boom"))

	require.Equal(t, "boom", ctx.Errors.Last().Err.Error())
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.NotContains(t, recorder.Body.String(), "boom")
}

func TestAbortRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Parallel()

	cases := []struct {
		err    error
		status int
		code   string
	}{
		{errors.New("basis: unknown"), 400, "bad_request"},
		{&http.MaxBytesError{}, 413, "request_too_large"},
		{api.ErrSessionNotFound, 404, "session_not_found"},
	}

	for _, tc := range cases {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		api.AbortRequest(ctx, tc.err)
		require.Equal(t, tc.status, recorder.Code)
		require.Contains(t, recorder.Body.String(), `"code":"`+tc.code+`"`)
	}
}

func TestCodes(t *testing.T) {
	t.Parallel()

	require.Equal(t, http.StatusPaymentRequired, api.ErrPrepayBelowMinimum.HTTPStatus)
	require.Equal(t, "prepay_below_minimum", api.ErrPrepayBelowMinimum.Code)
	require.Contains(t, api.Codes(), "channel_node_stale")
	require.Contains(t, api.Codes(), "conflict")
	require.Contains(t, api.Codes(), "prepay_below_minimum")
	require.NotContains(t, api.Codes(), "credit_exhausted")
	require.Equal(t, "prepay_below_minimum: prepay below node minimum", api.ErrPrepayBelowMinimum.Error())
}

func TestParseError(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", api.MaxErrorMessageBytes+1)

	cases := []struct {
		name   string
		body   string
		status int
		is     error
	}{
		{"known code", `{"error":{"code":"session_not_found","message":"gone"}}`, 404, api.ErrSessionNotFound},
		{"unknown code", `{"error":{"code":"made_up","message":"x"}}`, 502, &api.Error{Code: "made_up"}},
		{"malformed code", `{"error":{"code":"Made Up","message":"x"}}`, 502, api.ErrUnexpectedStatus},
		{"long message", `{"error":{"code":"session_not_found","message":"` + long + `"}}`, 404, api.ErrUnexpectedStatus},
		{"control byte", `{"error":{"code":"session_not_found","message":"a\u001bb"}}`, 404, api.ErrUnexpectedStatus},
		{"not json", `not json`, 502, api.ErrUnexpectedStatus},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := api.ParseError(tc.status, []byte(tc.body))
			require.ErrorIs(t, err, tc.is)

			if errors.Is(tc.is, api.ErrUnexpectedStatus) {
				require.Contains(t, err.Error(), strconv.Itoa(tc.status))
			}
		})
	}
}

func TestParseError_PrepayBelowMinimum(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(api.ErrorResponse{Error: *api.ErrPrepayBelowMinimum})
	require.NoError(t, err)

	got := api.ParseError(http.StatusPaymentRequired, body)

	require.ErrorIs(t, got, api.ErrPrepayBelowMinimum)
}
