package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
)

// A NIC flap with the kill switch on: the session ended without a Disconnect
// and traffic left directly. With the kill switch on, exhausted reconnects released the block
// (only "persist" kept it). The contract: on or persist, only the user's Disconnect lifts it.
func TestRuntime_ReconnectExhausted_KillSwitchStaysUntilDisconnect(t *testing.T) {
	t.Parallel()

	for _, ks := range []string{config.KillSwitchOn, config.KillSwitchPersist} {
		t.Run(ks, func(t *testing.T) {
			t.Parallel()

			rt, home, fw := exhaustingRuntime(t, ks)
			startIssuer(t, rt, home)
			require.NotEmpty(t, fw.applies(), "kill switch armed with the session")

			home.NodeService.SetOpenRejections(1000)
			home.NodeService.SetGetSessionErr(api.ErrInternal, 1000)

			require.Never(t, func() bool { return fw.flushes() > 0 }, 3*time.Second, 10*time.Millisecond,
				"a session that ended without Disconnect must keep the kill switch (fail closed)")

			require.NoError(t, client.StopSession(t.Context(), rt)) // no live session any more: only the block is left
			require.Eventually(t, func() bool { return fw.flushes() >= 1 }, 3*time.Second, 10*time.Millisecond,
				"the user's Disconnect removes it")
		})
	}
}

// Control: with the kill switch off, no firewall is armed, so exhausted reconnects leave nothing behind.
func TestRuntime_ReconnectExhausted_KillSwitchOffArmsNothing(t *testing.T) {
	t.Parallel()

	rt, home, fw := exhaustingRuntime(t, config.KillSwitchOff)
	startIssuer(t, rt, home)

	home.NodeService.SetOpenRejections(1000)
	home.NodeService.SetGetSessionErr(api.ErrInternal, 1000)

	// On this base "off" arms no kill switch at all, so nothing is ever applied — and therefore nothing
	// to leak or release when reconnecting gives up. (The fork's reconnect-hold-when-off is a separate
	// feature, not part of this fix.)
	require.Never(t, func() bool { return len(fw.applies()) > 0 }, 3*time.Second, 10*time.Millisecond,
		"kill switch off: no firewall is armed")
}

func exhaustingRuntime(t *testing.T, ks string) (*client.Runtime, testutil.Home, *recordingFirewallBackend) {
	t.Helper()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	home.Loaded.Client.MaxPrepayBytes = 100_000_000
	home.Loaded.Client.EscrowBaseUnits = 100_000
	home.Loaded.Client.Network.KillSwitch = ks
	home.Loaded.Client.NodeTimeoutSeconds = 2
	home.NodeService.SetEscrow(100_000)
	home.NodeService.SetHeartbeatInterval(0)

	rt := newRuntime(t, home, installOpenChannelReactor(t, fakeRPC, home))
	fw := &recordingFirewallBackend{}
	client.SetFirewallBackend(rt, fw)
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	registerFakeClientOnce()

	state := sdktest.NewFakeState(sdktest.FakeName)
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{state: state})
	client.SetTiming(rt, time.Hour, 50*time.Millisecond, 2, 30*time.Millisecond)

	return rt, home, fw
}
