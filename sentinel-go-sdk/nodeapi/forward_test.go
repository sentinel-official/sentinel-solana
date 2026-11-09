package nodeapi_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// A node on a newer version may add a field to its metadata. A client ignores it; a key that only differs from a
// declared one in case is still refused, because encoding/json would decode it into that field.
func TestDiscover_ForwardCompatibleMetadata(t *testing.T) {
	t.Parallel()

	ownerKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	operatorKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	fakeRPC := sdktest.NewFakeRPC(t)
	seedGrant(t, fakeRPC, ownerKey.PublicKey(), operatorKey.PublicKey())

	programClient := program.NewClient(fakeRPC.NewClient(), dvpn.ProgramID, solana.PublicKey{}, rpc.CommitmentConfirmed)
	nodeAddress := solana.PublicKey{2}

	cases := []struct {
		err  error
		key  string
		name string
	}{
		{key: "voucher_rate_per_second", name: "unknown settlement field"},
		{err: api.ErrInvalidResponse, key: "Heartbeat_Seconds", name: "case variant settlement field"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			metadata := metadataFixture(nodeAddress, ownerKey.PublicKey(), operatorKey.PublicKey(), dvpn.ProgramID, time.Now())

			raw, err := json.Marshal(metadata)
			require.NoError(t, err)

			var payload map[string]any
			require.NoError(t, json.Unmarshal(raw, &payload))

			settlement, ok := payload["settlement"].(map[string]any)
			require.True(t, ok)

			settlement[tc.key] = 5

			signed, err := auth.SignMessage(operatorKey, auth.DomainNodeMetadata, payload)
			require.NoError(t, err)

			srv := startSignedMetadataServer(t, signed)

			node := dvpn.Node{Owner: ownerKey.PublicKey(), URILen: uint8(len(srv.URL))}
			copy(node.URI[:], srv.URL)

			got, _, err := nodeapi.Discover(t.Context(), programClient, nodeAddress, node, time.Second)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, metadata.Settlement, got.Settlement)
		})
	}
}

func TestNewInsecureClient_ForwardCompatibleResponses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		body   string
		check  func(t *testing.T, resp *nodeapi.GetHealthResponse, err error)
		name   string
		status int
	}{
		{
			body: `{"status":"ok","version":"0","uptime_seconds":5}`,
			check: func(t *testing.T, resp *nodeapi.GetHealthResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				require.NotNil(t, resp.JSON200)
				require.Equal(t, "0", resp.JSON200.Version)
			},
			name:   "unknown field",
			status: http.StatusOK,
		},
		{
			body: `{"status":"ok","version":"0","Version":"1"}`,
			check: func(t *testing.T, _ *nodeapi.GetHealthResponse, err error) {
				t.Helper()
				require.ErrorIs(t, err, api.ErrInvalidResponse)
			},
			name:   "case variant field",
			status: http.StatusOK,
		},
		{
			body: `{"error":{"code":"ticket_request_stale","message":"ticket request is stale","retry_after":3}}`,
			check: func(t *testing.T, resp *nodeapi.GetHealthResponse, err error) {
				t.Helper()
				require.NoError(t, err)

				var apiErr *api.Error
				require.True(t, errors.As(api.ParseError(resp.StatusCode(), resp.Body), &apiErr))
				require.Equal(t, "ticket_request_stale", apiErr.Code)
				require.Equal(t, http.StatusConflict, apiErr.HTTPStatus)
				require.Equal(t, "ticket request is stale", apiErr.Message)
			},
			name:   "unknown error code",
			status: http.StatusConflict,
		},
		{
			body: `{"error":{"code":"Bad Code!","message":"x"}}`,
			check: func(t *testing.T, _ *nodeapi.GetHealthResponse, err error) {
				t.Helper()
				require.ErrorIs(t, err, api.ErrInvalidResponse)
			},
			name:   "malformed error code",
			status: http.StatusConflict,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}

			srv := httptest.NewServer(http.HandlerFunc(handler))
			t.Cleanup(srv.Close)

			client, err := nodeapi.NewInsecureClient(srv.URL, time.Second)
			require.NoError(t, err)

			resp, err := client.GetHealthWithResponse(t.Context())
			tc.check(t, resp, err)
		})
	}
}
