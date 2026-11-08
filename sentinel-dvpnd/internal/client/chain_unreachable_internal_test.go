package client

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/solana-foundation/solana-go/v2/rpc/jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestChainUnreachable(t *testing.T) {
	t.Parallel()

	refused := &url.Error{Op: "Post", URL: "http://127.0.0.1:1", Err: errors.New("connection refused")}

	cases := []struct {
		err  error
		name string
		want bool
	}{
		{name: "nil"},
		{name: "connection refused", err: fmt.Errorf("rpc call getAccountInfo(): %w", refused), want: true},
		{name: "http 429", err: &jsonrpc.HTTPError{Code: http.StatusTooManyRequests}, want: true},
		{name: "http 503", err: &jsonrpc.HTTPError{Code: http.StatusServiceUnavailable}, want: true},
		{name: "http 400", err: &jsonrpc.HTTPError{Code: http.StatusBadRequest}},
		{name: "json-rpc rate limit", err: fmt.Errorf("wrapped: %w", &jsonrpc.RPCError{Code: rpcRateLimitCode}), want: true},
		{name: "json-rpc invalid params", err: &jsonrpc.RPCError{Code: -32602}},
		{name: "other error", err: errors.New("account not found")},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, chainUnreachable(tc.err), tc.name)
	}
}
