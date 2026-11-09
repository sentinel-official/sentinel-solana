package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
)

// Killing the tunnel process mid-transfer with the kill switch on ended the
// session with a node reason and traffic went direct straightaway. A node-initiated end (here an
// overcharge abort) must NOT lift the kill switch when it is on/persist; only the user's own
// disconnect does. This exercises the teardown() release path, distinct from exhausted reconnect.
func TestRuntime_NodeEndedSession_KillSwitchStaysUntilDisconnect(t *testing.T) {
	t.Parallel()

	for _, ks := range []string{config.KillSwitchOn, config.KillSwitchPersist} {
		t.Run(ks, func(t *testing.T) {
			t.Parallel()

			rt, home, fw, state := endingRuntime(t, ks)
			row := startIssuer(t, rt, home)
			require.NotEmpty(t, fw.applies(), "kill switch armed with the session")

			// Drive a node-initiated end (overcharge) — the session ends with reason "node overcharging".
			state.SetLocalStats(service.PeerStats{RxBytes: 1_000_000_000})
			home.NodeService.SetUsage(session.Usage{RxBytes: 1_060_000_000})
			home.NodeService.PublishVoucherRequest()
			waitIssuerEnded(t, home, row.ID, "node overcharging")

			require.Never(t, func() bool { return fw.flushes() > 0 }, 2*time.Second, 10*time.Millisecond,
				"a node-ended session must keep the kill switch (fail closed) until the user disconnects")

			require.NoError(t, client.StopSession(t.Context(), rt)) // the user's explicit disconnect (leftover block)
			require.Eventually(t, func() bool { return fw.flushes() >= 1 }, 2*time.Second, 10*time.Millisecond,
				"the user's Disconnect removes it")
		})
	}
}

// Control: with the kill switch off, a node-ended session releases whatever (nothing) was armed and
// never holds the machine offline.
func TestRuntime_NodeEndedSession_KillSwitchOffReleases(t *testing.T) {
	t.Parallel()

	rt, home, fw, state := endingRuntime(t, config.KillSwitchOff)
	row := startIssuer(t, rt, home)

	state.SetLocalStats(service.PeerStats{RxBytes: 1_000_000_000})
	home.NodeService.SetUsage(session.Usage{RxBytes: 1_060_000_000})
	home.NodeService.PublishVoucherRequest()
	waitIssuerEnded(t, home, row.ID, "node overcharging")

	// off arms no firewall, so nothing is ever applied.
	require.Never(t, func() bool { return len(fw.applies()) > 0 }, 2*time.Second, 10*time.Millisecond,
		"kill switch off: no firewall is armed")
}

func endingRuntime(t *testing.T, ks string) (*client.Runtime, testutil.Home, *recordingFirewallBackend, *sdktest.FakeState) {
	t.Helper()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	home.Loaded.Client.MaxPrepayBytes = 100_000_000
	home.Loaded.Client.EscrowBaseUnits = 100_000
	home.Loaded.Client.Network.KillSwitch = ks
	home.NodeService.SetEscrow(100_000)
	home.NodeService.SetHeartbeatInterval(0)

	rt := newRuntime(t, home, installOpenChannelReactor(t, fakeRPC, home))
	fw := &recordingFirewallBackend{}
	client.SetFirewallBackend(rt, fw)
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	registerFakeClientOnce()

	state := sdktest.NewFakeState(sdktest.FakeName)
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{state: state})

	return rt, home, fw, state
}
