//go:build linux

package node_test

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/node"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// fakeLeftoverServer is a server whose old interface outlived the daemon, as a kernel WireGuard
// interface does.
type fakeLeftoverServer struct {
	*sdktest.FakeServer

	mu       sync.Mutex
	leftover map[service.PeerID]service.PeerStats
}

func (s *fakeLeftoverServer) LeftoverPeerStats(context.Context) (map[service.PeerID]service.PeerStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.leftover == nil {
		return nil, errors.New("no leftover interface")
	}

	return maps.Clone(s.leftover), nil
}

// leftoverRemover drops the old interface and its counters with it, as removing the real one does.
type leftoverRemover struct {
	server *fakeLeftoverServer
}

func (r leftoverRemover) RemoveInterface(context.Context, string) error {
	r.server.mu.Lock()
	r.server.leftover = nil
	r.server.mu.Unlock()

	return nil
}

func TestRuntime_ClearLeftovers_MetersTrafficFromBeforeRestart(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeStream)
	req := nodeapi.OpenSessionRequest{
		Credential: nodeapi.Credential{Channel: &channel, Type: nodeapi.CredentialTypeChannel},
	}
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	opened, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)

	// The meter saw 18 MB before the daemon died; the kernel peer carried on to 53 MB.
	require.NoError(t, home.DB.Model(&node.Session{}).Where("id = ?", opened.ID).
		Updates(map[string]any{"rx_bytes": 18_000_000, "rx_counter": 18_000_000}).Error)

	srv := &fakeLeftoverServer{
		FakeServer: sdktest.NewFakeServer(state),
		leftover: map[service.PeerID]service.PeerStats{
			service.PeerID(opened.PeerID): {RxBytes: 53_000_000, TxBytes: 400_000},
		},
	}
	node.SetService(r, srv)
	node.SetInterfaceRemover(r, leftoverRemover{server: srv})
	node.SetServerCleanup(r, fakeServerCleanup)

	require.NoError(t, node.ClearLeftovers(t.Context(), r))

	var got node.Session
	require.NoError(t, home.DB.First(&got, "id = ?", opened.ID).Error)
	require.Equal(t, uint64(53_000_000), got.RxBytes)
	require.Equal(t, uint64(400_000), got.TxBytes)
	require.Zero(t, got.RxCounter, "the replacement interface counts from zero")
	require.Zero(t, got.TxCounter)
}

func TestRuntime_ClearLeftovers_WithoutLeftoverInterfaceKeepsUsage(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeStream)
	req := nodeapi.OpenSessionRequest{
		Credential: nodeapi.Credential{Channel: &channel, Type: nodeapi.CredentialTypeChannel},
	}
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	opened, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)
	require.NoError(t, home.DB.Model(&node.Session{}).Where("id = ?", opened.ID).
		Updates(map[string]any{"rx_bytes": 18_000_000, "rx_counter": 18_000_000}).Error)

	srv := &fakeLeftoverServer{FakeServer: sdktest.NewFakeServer(state)}
	node.SetService(r, srv)
	node.SetInterfaceRemover(r, leftoverRemover{server: srv})
	node.SetServerCleanup(r, fakeServerCleanup)

	require.NoError(t, node.ClearLeftovers(t.Context(), r))

	var got node.Session
	require.NoError(t, home.DB.First(&got, "id = ?", opened.ID).Error)
	require.Equal(t, uint64(18_000_000), got.RxBytes)
	require.Equal(t, uint64(18_000_000), got.RxCounter)
}
