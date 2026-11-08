package client

import (
	"errors"
	"net/http"
	"net/url"

	"github.com/solana-foundation/solana-go/v2/rpc/jsonrpc"
)

// rpcRateLimitCode is the JSON-RPC error code an RPC uses for rate limiting, sent inside an HTTP 200.
const rpcRateLimitCode = -32429

// errChainUnreachable is what the user sees when a session cannot start because the chain RPC did not
// answer. The raw RPC error (with the RPC URL) goes to the daemon log only.
var errChainUnreachable = errors.New("the network is unreachable or busy right now; try again in a moment")

// chainUnreachable reports whether err from a chain read means the RPC could not be reached or refused
// the read for now (no connection, a timeout, HTTP 429 or 5xx, or a JSON-RPC rate limit), rather than
// answering that something is wrong.
func chainUnreachable(err error) bool {
	if err == nil {
		return false
	}

	var rpcErr *jsonrpc.RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.Code == http.StatusTooManyRequests || rpcErr.Code == rpcRateLimitCode
	}

	var httpErr *jsonrpc.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Code == http.StatusTooManyRequests || httpErr.Code >= http.StatusInternalServerError
	}

	var urlErr *url.Error

	return errors.As(err, &urlErr)
}
