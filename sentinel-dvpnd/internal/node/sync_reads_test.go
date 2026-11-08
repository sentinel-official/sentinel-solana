//go:build linux

package node_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/solana-foundation/solana-go/v2/rpc/jsonrpc"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/node"
	"github.com/sentinel-official/sentinel-dvpnd/internal/store"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/wireguard"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// readCountTransport counts account reads by JSON-RPC method.
type readCountTransport struct {
	base   http.RoundTripper
	mu     sync.Mutex
	counts map[string]int
}

func (t *readCountTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("reading RPC request: %w", err)
	}

	for _, method := range []string{"getAccountInfo", "getMultipleAccounts"} {
		if bytes.Contains(body, []byte(`"method":"`+method+`"`)) {
			t.mu.Lock()
			t.counts[method]++
			t.mu.Unlock()
		}
	}

	req.Body = io.NopCloser(bytes.NewReader(body))

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, fmt.Errorf("sending RPC request: %w", err)
	}

	return resp, nil
}

func (t *readCountTransport) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.counts = map[string]int{}
}

func (t *readCountTransport) count(method string) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.counts[method]
}

// sync_positions runs every minute over every non-closed position. It must cost one batch per
// 100 positions, not one getAccountInfo each: ddv-node-1 held ~1,435 such positions across six
// nodes, about 2 M reads a day. Every third position here is already gone from chain.
func TestRuntime_SyncPositions_BatchesReads(t *testing.T) {
	t.Parallel()

	const n = 150

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewNodeHome(t, fakeRPC)

	transport := &readCountTransport{base: http.DefaultTransport, counts: map[string]int{}}
	rpcClient := rpc.NewWithCustomRPCClient(jsonrpc.NewClientWithOpts(fakeRPC.Server.URL, &jsonrpc.RPCClientOpts{
		HTTPClient: &http.Client{Transport: transport},
	}))
	programClient := program.NewClient(
		rpcClient, home.ProgramClient.ProgramID, home.ProgramClient.Mint, home.ProgramClient.Commitment,
	)

	r, err := node.New(
		home.Loaded, home.DB, home.DaemonKey, programClient, home.Sender, zerolog.Nop(), prometheus.NewRegistry(),
	)
	require.NoError(t, err)
	require.NoError(t, store.Migrate(t.Context(), home.DB, r.Models()...))
	require.NoError(t, node.SyncChain(t.Context(), r))
	node.SetService(r, sdktest.NewFakeServer(sdktest.NewFakeState(wireguard.Name)))

	_, channel := channelFixture(
		t, r, home, home.OwnerKey, home.OwnerAddress, home.OwnerAddress, dvpn.PositionModeVoucher,
	)

	address := func(i int) solana.PublicKey { return solana.PublicKey{byte(i / 256), byte(i % 256), 7} }

	for i := range n {
		seedPosition(t, r, home, address(i), 0)

		if i%3 == 0 {
			continue
		}

		home.RPC.SetAccount(address(i), sdktest.Account{
			Data:  sdktest.EncodeAccount(dvpn.ChannelAccountDiscriminator, channel),
			Owner: home.ProgramClient.ProgramID,
		})
	}

	transport.reset()
	require.NoError(t, node.SyncPositions(t.Context(), r))

	require.Equal(t, 2, transport.count("getMultipleAccounts"), "150 positions = 2 batches of at most 100")
	require.Zero(t, transport.count("getAccountInfo"), "no per-position read")

	for i := range n {
		var stored node.Position
		require.NoError(t, home.DB.WithContext(t.Context()).First(&stored, "address = ?", address(i).String()).Error)

		want := node.PositionStatusOpen
		if i%3 == 0 {
			want = node.PositionStatusClosed
		}

		require.Equal(t, want, stored.Status, "position %d", i)
	}
}
