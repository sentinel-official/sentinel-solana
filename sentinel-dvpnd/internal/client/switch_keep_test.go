package client_test

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// A server switch is StopSession then StartSession. With keep_kill_switch the block stays in place
// between the two, so nothing leaves outside the tunnel; the next session takes it over and only a
// plain StopSession releases it.
func TestGRPCServer_StopSession_KeepKillSwitchHoldsBlockForSwitch(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{config.KillSwitchOn, config.KillSwitchPersist} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			fixture := newKillSwitchFixtureMode(t, mode)
			require.NoError(t, fixture.rt.Recover(t.Context()))
			svc := startClientService(t, fixture.rt)

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()

			fixture.startAndSettle(t, ctx)
			require.NotEmpty(t, firewallRows(t, fixture.home))

			// Flushes before the first session (start-up recovery) are not the switch's.
			before := fixture.firewall.flushes()

			stopped, err := svc.StopSession(ctx, &daemonv1.StopSessionRequest{KeepKillSwitch: true})
			require.NoError(t, err)
			require.Equal(t, client.SessionStatusEnded, stopped.GetSession().GetStatus())
			require.False(t, stopped.GetKillSwitchReleased(), "the switch stop released the kill switch")
			require.Equal(t, before, fixture.firewall.flushes(), "the block was flushed between the two sessions")
			require.NotEmpty(t, firewallRows(t, fixture.home))

			// A second keep stop with no session leaves the held block alone too.
			_, err = svc.StopSession(ctx, &daemonv1.StopSessionRequest{KeepKillSwitch: true})
			require.NoError(t, err)
			require.Equal(t, before, fixture.firewall.flushes(), "a keep stop with no session flushed the held block")
			require.NotEmpty(t, firewallRows(t, fixture.home))

			fixture.startAndSettle(t, ctx)
			require.Equal(t, before, fixture.firewall.flushes(), "the next session did not take the block over")

			released, err := svc.StopSession(ctx, &daemonv1.StopSessionRequest{})
			require.NoError(t, err)
			require.True(t, released.GetKillSwitchReleased())
			require.Equal(t, before+1, fixture.firewall.flushes())
			require.Empty(t, firewallRows(t, fixture.home))
		})
	}
}

// With the kill switch off there is nothing to keep: keep_kill_switch is a plain stop.
func TestGRPCServer_StopSession_KeepKillSwitchOffIsPlainStop(t *testing.T) {
	t.Parallel()

	fixture := newKillSwitchFixtureMode(t, config.KillSwitchOff)
	require.NoError(t, fixture.rt.Recover(t.Context()))
	svc := startClientService(t, fixture.rt)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	fixture.startAndSettle(t, ctx)

	stopped, err := svc.StopSession(ctx, &daemonv1.StopSessionRequest{KeepKillSwitch: true})
	require.NoError(t, err)
	require.Equal(t, client.SessionStatusEnded, stopped.GetSession().GetStatus())
	require.Empty(t, fixture.firewall.applies())
	require.Empty(t, firewallRows(t, fixture.home))
}

// A switch to a node on another IP address under a kept block: StartSession admits that address to
// the block before dialing (keeping everything it admitted), without lifting it.
func TestRuntime_KeptBlockAdmitsAnotherNodeAddress(t *testing.T) {
	t.Parallel()

	fixture := newKillSwitchFixtureMode(t, config.KillSwitchOn)
	require.NoError(t, fixture.rt.Recover(t.Context()))
	svc := startClientService(t, fixture.rt)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	fixture.startAndSettle(t, ctx)

	_, err := svc.StopSession(ctx, &daemonv1.StopSessionRequest{KeepKillSwitch: true})
	require.NoError(t, err)

	before, applies := fixture.firewall.flushes(), len(fixture.firewall.applies())
	held := fixture.firewall.applies()[applies-1].nodeAddrs
	other := netip.MustParseAddr("198.51.100.7")

	addrs, err := client.ResolveHost(ctx, fixture.rt, other.String())
	require.NoError(t, err)
	require.Equal(t, []netip.Addr{other}, addrs)

	got := fixture.firewall.applies()
	require.Len(t, got, applies+1, "the other node's address was not admitted to the block")
	require.Subset(t, got[applies].nodeAddrs, held, "admitting the other node dropped what the block admitted")
	require.Contains(t, got[applies].nodeAddrs, other)
	require.Equal(t, before, fixture.firewall.flushes(), "the block was lifted to admit the other node")

	// A host name cannot be resolved under the block: refused, nothing lifted.
	_, err = client.ResolveHost(ctx, fixture.rt, "other-node.example")
	require.ErrorIs(t, err, client.ErrNodeNotAdmitted)
	require.Equal(t, before, fixture.firewall.flushes())
}

// next_node: the next node's host name is resolved while the session being stopped is still up and
// admitted to the kept block, so the next session reaches it with no lookup under the block.
func TestGRPCServer_StopSession_NextNodeAdmitsHostName(t *testing.T) {
	t.Parallel()

	fixture := newKillSwitchFixtureMode(t, config.KillSwitchOn)
	require.NoError(t, fixture.rt.Recover(t.Context()))
	svc := startClientService(t, fixture.rt)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	fixture.startAndSettle(t, ctx)

	const host = "next-node.example"
	next := solana.NewWallet().PublicKey()
	nextAddr := netip.MustParseAddr("203.0.113.9")

	var uri [128]uint8
	raw := "https://" + host + ":18443"
	copy(uri[:], raw)
	fixture.home.RPC.SetNode(fixture.home.ProgramClient.ProgramID, next, dvpn.Node{
		OpenedAt: time.Now().Unix(),
		Status:   dvpn.ListingStatusListed,
		URI:      uri,
		URILen:   uint8(len(raw)),
	})

	// The live session's DNS goes through its tunnel; once only the block is left, none may run.
	var mu sync.Mutex
	stopped, lookupsAfterStop := false, 0
	client.SetLookup(fixture.rt, func(_ context.Context, _, name string) ([]netip.Addr, error) {
		mu.Lock()
		defer mu.Unlock()
		if stopped {
			lookupsAfterStop++
		}
		if name == host {
			return []netip.Addr{nextAddr}, nil
		}

		return nil, errors.New("no such host: " + name)
	})

	before, applies := fixture.firewall.flushes(), len(fixture.firewall.applies())

	_, err := svc.StopSession(ctx, &daemonv1.StopSessionRequest{KeepKillSwitch: true, NextNode: next.String()})
	require.NoError(t, err)

	mu.Lock()
	stopped = true
	mu.Unlock()

	got := fixture.firewall.applies()
	require.Greater(t, len(got), applies, "the next node was not admitted to the block")
	require.Contains(t, got[len(got)-1].nodeAddrs, nextAddr)
	require.Equal(t, before, fixture.firewall.flushes(), "the block was lifted")

	addrs, err := client.ResolveHost(ctx, fixture.rt, host)
	require.NoError(t, err)
	require.Equal(t, []netip.Addr{nextAddr}, addrs)

	mu.Lock()
	defer mu.Unlock()
	require.Zero(t, lookupsAfterStop, "a DNS lookup ran under the block")
}

// next_node that cannot be resolved: the session is not stopped (the user stays protected and
// connected) and the caller gets the error.
func TestGRPCServer_StopSession_NextNodeUnresolvedKeepsSession(t *testing.T) {
	t.Parallel()

	fixture := newKillSwitchFixtureMode(t, config.KillSwitchOn)
	require.NoError(t, fixture.rt.Recover(t.Context()))
	svc := startClientService(t, fixture.rt)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	fixture.startAndSettle(t, ctx)

	_, err := svc.StopSession(ctx, &daemonv1.StopSessionRequest{KeepKillSwitch: true, NextNode: "not-a-key"})
	require.Error(t, err)

	_, active := client.Current(fixture.rt)
	require.True(t, active, "a failed next_node stopped the session anyway")
}
