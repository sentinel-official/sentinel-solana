package client_test

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
)

// A node that is slow to answer the session poll must not stop usage sampling (and with it voucher issuing).
func TestRuntime_SlowPollDoesNotStallSampling(t *testing.T) { //nolint:paralleltest // timing
	rt, home, state := newIssuerFixture(t, 100_000, false)

	var samples atomic.Int64
	count := func() error { samples.Add(1); return nil }
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{hooks: clientHooks{onStatsError: &count}, state: state})
	client.SetTiming(rt, 20*time.Millisecond, 50*time.Millisecond, 0, time.Millisecond)

	var hang atomic.Bool
	release := make(chan struct{})
	handler := home.NodeServer.Config.Handler
	home.NodeServer.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/sessions/") &&
			!strings.HasSuffix(r.URL.Path, "/events") && hang.CompareAndSwap(true, false) {
			select {
			case <-release:
			case <-time.After(3 * time.Second):
			}
		}
		handler.ServeHTTP(w, r)
	})
	t.Cleanup(func() { close(release) })

	startIssuer(t, rt, home)
	time.Sleep(300 * time.Millisecond)

	hang.Store(true)
	require.Eventually(t, func() bool { return !hang.Load() }, time.Second, time.Millisecond, "the poll reached the node")
	before := samples.Load()
	time.Sleep(time.Second)

	// At a 20 ms interval about 50 samples fit in the second; a blocked loop manages at most one or two.
	require.GreaterOrEqual(t, samples.Load()-before, int64(20), "samples kept running while the poll hung")
}
