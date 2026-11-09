package client

import (
	"testing"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
)

// keepsFirewall encodes the kill-switch contract: with "on", a session
// that ends for any reason the user did not ask for keeps the block (billing ends included — escrow
// exhausted, node overcharging, the node closing/forgetting the session, a daemon restart, the tunnel
// path dying); "persist" keeps it for every non-user end; only the user's disconnect and ks=off lift
// it.
func TestKeepsFirewall(t *testing.T) {
	cases := []struct {
		reason, ks string
		keep       bool
	}{
		// A dying path / unreadable meter is held like "connection lost"; only the user lifts it.
		{reasonStatsUnavailable, config.KillSwitchOn, true},
		{reasonStatsUnavailable, config.KillSwitchPersist, true},
		{reasonConnectionLost, config.KillSwitchOn, true},
		// "on" keeps the block when billing ends too.
		{reasonEscrowExhausted, config.KillSwitchOn, true},
		{reasonNodeOvercharging, config.KillSwitchOn, true},
		{reasonInvalidResponse, config.KillSwitchOn, true},
		{"session not found", config.KillSwitchOn, true}, // default case: any unlisted node reason
		{reasonDaemonRestarted, config.KillSwitchOn, true},
		// persist keeps the block on every end the user did not ask for.
		{reasonNodeOvercharging, config.KillSwitchPersist, true},
		{reasonEscrowExhausted, config.KillSwitchPersist, true},
		{"session not found", config.KillSwitchPersist, true},
		{reasonDaemonRestarted, config.KillSwitchPersist, true},
		// Daemon stopping: only persist keeps it.
		{reasonDaemonStopping, config.KillSwitchOn, false},
		{reasonDaemonStopping, config.KillSwitchPersist, true},
		// A server switch (StopSession with keep_kill_switch): the next session takes the block over,
		// with the kill switch on or persist; off holds nothing (#53).
		{reasonSwitching, config.KillSwitchOn, true},
		{reasonSwitching, config.KillSwitchPersist, true},
		{reasonSwitching, config.KillSwitchOff, false},
		// Negative controls: the user's disconnect always lifts it; ks=off holds nothing.
		{reasonStoppedByUser, config.KillSwitchOn, false},
		{reasonStoppedByUser, config.KillSwitchPersist, false},
		{reasonEscrowExhausted, config.KillSwitchOff, false},
		{reasonNodeOvercharging, config.KillSwitchOff, false},
		{"session not found", config.KillSwitchOff, false},
	}
	for _, c := range cases {
		if got := keepsFirewall(c.reason, c.ks); got != c.keep {
			t.Errorf("keepsFirewall(%q, %q) = %v, want %v", c.reason, c.ks, got, c.keep)
		}
	}
}
