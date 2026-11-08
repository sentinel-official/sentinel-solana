package client_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
)

// The tunnel process was killed under a live session: its stats read failed at once, but status kept
// reading a healthy "active" with no error until the session ended maxStatsFailures samples later
// (5 s at the default 1 s sample). The first failed read now shows it; the next good read clears it.
func TestRuntime_IssueLoop_ShowsAFailingTunnelWhileTheSessionIsActive(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	client.SetTiming(rt, time.Second, time.Hour, 8, 30*time.Millisecond)

	var failing atomic.Bool

	statsError := func() error {
		if failing.Load() {
			return errors.New("peer counters unavailable")
		}

		return nil
	}
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{
		hooks: clientHooks{onStatsError: &statsError},
		state: state,
	})
	row := startIssuer(t, rt, home)

	reads := func(want string) func() bool {
		return func() bool {
			current, ok := client.Current(rt)

			return ok && current.ID == row.ID && current.Status == client.SessionStatusActive && current.Error == want
		}
	}

	require.Eventually(t, reads(""), 2*time.Second, 10*time.Millisecond)

	failing.Store(true)
	require.Eventually(t, reads("service stats unavailable"), 2*time.Second, 10*time.Millisecond,
		"a tunnel that stopped answering must not read healthy for more than 2 s")

	failing.Store(false)
	require.Eventually(t, reads(""), 3*time.Second, 10*time.Millisecond, "the next good read clears it")
}
