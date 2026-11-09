//go:build linux

package provider_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/provider"
	"github.com/sentinel-official/sentinel-dvpnd/internal/store"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
)

// busyRPC answers the next requests with HTTP 429, as a rate-limited public RPC does, then passes
// them to the fake.
type busyRPC struct {
	next    http.Handler
	left    atomic.Int64
	limited atomic.Int64
}

func (b *busyRPC) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if b.left.Add(-1) >= 0 {
		b.limited.Add(1)
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)

		return
	}

	b.next.ServeHTTP(w, req)
}

func newProviderBehindBusyRPC(t *testing.T) (*provider.Runtime, *busyRPC) {
	t.Helper()

	fakeRPC := sdktest.NewFakeRPC(t)
	busy := &busyRPC{next: fakeRPC}
	fakeRPC.Server.Close()
	fakeRPC.Server = httptest.NewServer(busy)
	t.Cleanup(fakeRPC.Server.Close)

	home := testutil.NewProviderHome(t, fakeRPC)
	r, err := provider.New(
		home.Loaded, home.DB, home.DaemonKey, home.ProgramClient, home.Sender, zerolog.Nop(), prometheus.NewRegistry(),
	)
	require.NoError(t, err)
	require.NoError(t, store.Migrate(t.Context(), home.DB, r.Models()...))

	return r, busy
}

// A provider whose first chain read at Start got a 429 exited and was restarted by its supervisor,
// although the RPC answered again seconds later. Start waits it out.
func TestStart_WaitsOutABusyRPC(t *testing.T) {
	t.Cleanup(provider.SetStartChainRetry(time.Millisecond, 5*time.Millisecond))

	r, busy := newProviderBehindBusyRPC(t)
	t.Cleanup(func() { require.NoError(t, r.Stop(context.Background())) })
	busy.left.Store(3)

	require.NoError(t, r.Start(t.Context()), "a busy RPC at start is waited out")
	require.EqualValues(t, 3, busy.limited.Load())
}

func TestStart_StopsWaitingWhenCancelled(t *testing.T) {
	t.Cleanup(provider.SetStartChainRetry(time.Millisecond, 5*time.Millisecond))

	r, busy := newProviderBehindBusyRPC(t)
	busy.left.Store(1 << 40)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	err := r.Start(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
