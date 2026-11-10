package main_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	main "github.com/sentinel-official/sentinel-dvpnd/cmd/sentinel-dvpnd"
	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/daemon"
	"github.com/sentinel-official/sentinel-dvpnd/internal/preflight"
	"github.com/sentinel-official/sentinel-dvpnd/internal/worker"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

type fakeServeRuntime struct {
	daemon.Runtime

	started *atomic.Bool
}

func (fakeServeRuntime) Jobs() []worker.Job { return nil }

func (fakeServeRuntime) Register(grpc.ServiceRegistrar) {}

func (fakeServeRuntime) Serve(ctx context.Context) error {
	<-ctx.Done()

	return nil
}

func (r fakeServeRuntime) Start(context.Context) error {
	r.started.Store(true)

	return nil
}

func (fakeServeRuntime) Stop(context.Context) error { return nil }

func TestPrepareRuntime_DNSCleanupFailureStopsBeforePreflight(t *testing.T) {
	t.Parallel()

	cause := errors.New("resolver restore failed")
	rt := fakeRecoverRuntime{recoverErr: errors.Join(client.ErrDNSCleanupFailed, cause)}

	err := main.PrepareRuntime(t.Context(), rt)
	require.ErrorIs(t, err, client.ErrDNSCleanupFailed)
	require.ErrorIs(t, err, cause)
}

func TestPrepareRuntime_ServiceCleanupFailureStopsBeforePreflight(t *testing.T) {
	t.Parallel()

	cause := errors.New("tunnel cleanup failed")
	rt := fakeRecoverRuntime{recoverErr: errors.Join(client.ErrServiceCleanupFailed, cause)}

	err := main.PrepareRuntime(t.Context(), rt)
	require.ErrorIs(t, err, client.ErrServiceCleanupFailed)
	require.ErrorIs(t, err, cause)
}

func TestServe_MetricsBindFailureFailsBeforeReady(t *testing.T) {
	t.Parallel()

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	loaded := &config.Loaded{Dir: t.TempDir()}
	loaded.File.Metrics = config.Metrics{
		Enabled: true,
		Listen:  listener.Addr().String(),
	}

	var ready, started atomic.Bool

	err = main.Serve(t.Context(), loaded, fakeServeRuntime{started: &started}, func() { ready.Store(true) })

	var bindErr *net.OpError
	require.ErrorAs(t, err, &bindErr)
	require.Equal(t, "listen", bindErr.Op)
	require.False(t, started.Load())
	require.False(t, ready.Load())
}

// ─── A client that starts without the RPC (private I#17) ───

// unreachableLoaded pins the build identity, a package global: its callers must not run in parallel.
func unreachableLoaded(t *testing.T, role string) *config.Loaded {
	t.Helper()

	sdktest.PinBuildIdentity(t, dvpn.ProgramID, solana.PublicKey{1})

	file := config.DefaultFile("localnet")
	file.RPC.URL = "http://127.0.0.1:1"
	file.RPC.WSURL = "ws://127.0.0.1:1"
	file.RPC.Timeout = 100 * time.Millisecond

	loaded := &config.Loaded{File: file, Role: role}
	if role == config.RoleClient {
		c := config.DefaultClient()
		c.Network.DNSBackend = config.DNSNone
		loaded.Client = &c
	} else {
		loaded.Node = &config.Node{Service: "wireguard"}
	}

	return loaded
}

func TestPrepareStart_ClientServesWhenTheRPCIsUnreachable(t *testing.T) {
	// Not parallel: unreachableLoaded pins the build identity (version.ProgramID/Mint, package
	// globals), which other tests here read through version.Current() (-race on the stack).

	loaded := unreachableLoaded(t, config.RoleClient)

	waits, err := main.PrepareStart(t.Context(), loaded, rpc.New(loaded.File.RPC.URL))
	require.NoError(t, err, "an unreachable rpc must not fail the client's start")
	require.True(t, waits, "the startup checks must move to the background")
}

func TestPrepareStart_NodeWaitsForTheRPCAndFailsOnlyWhenTheStartIsCancelled(t *testing.T) {
	// Not parallel: unreachableLoaded pins the build identity (version.ProgramID/Mint, package
	// globals), which other tests here read through version.Current() (-race on the stack).

	// A node waits for an unreachable RPC (it does not exit and crash-loop under its service manager); the
	// client path serves and waits in the background instead. Only the end of the start (ctx) ends the wait,
	// with the last error.
	loaded := unreachableLoaded(t, config.RoleNode)

	ctx, cancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer cancel()

	began := time.Now()
	waits, err := main.PrepareStart(ctx, loaded, rpc.New(loaded.File.RPC.URL))

	require.ErrorIs(t, err, preflight.ErrRPCUnreachable)
	require.False(t, waits)
	require.GreaterOrEqual(t, time.Since(began), 500*time.Millisecond, "the node waited instead of exiting at the first failure")
}

func TestPrepareStart_ClientWithAnInvalidRPCConfigStillFails(t *testing.T) {
	// Not parallel: unreachableLoaded pins the build identity (version.ProgramID/Mint, package
	// globals), which other tests here read through version.Current() (-race on the stack).

	loaded := unreachableLoaded(t, config.RoleClient)
	loaded.File.RPC.URL = ""

	waits, err := main.PrepareStart(t.Context(), loaded, rpc.New("http://127.0.0.1:1"))
	require.ErrorIs(t, err, preflight.ErrPreflightFailed)
	require.NotErrorIs(t, err, preflight.ErrRPCUnreachable, "a config error is not a network error")
	require.False(t, waits)
}

func TestWaitForChainAccess_StopsTheDaemonWhenTheRPCAnswersButACheckFails(t *testing.T) {
	// Not parallel: unreachableLoaded pins the build identity (version.ProgramID/Mint, package
	// globals), which other tests here read through version.Current() (-race on the stack).

	fakeRPC := sdktest.NewFakeRPC(t)
	sdktest.PinBuildIdentity(t, dvpn.ProgramID, solana.PublicKey{1})
	require.NoError(t, fakeRPC.SetConfig(dvpn.ProgramID, dvpn.Config{Mint: solana.PublicKey{2}}))

	target, err := url.Parse(fakeRPC.Config().URL)
	require.NoError(t, err)

	proxy := httputil.NewSingleHostReverseProxy(target)

	var refused atomic.Int32

	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refused.Load() < 2 {
			refused.Add(1)
			http.Error(w, "no network yet", http.StatusServiceUnavailable)

			return
		}

		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(gate.Close)

	loaded := unreachableLoaded(t, config.RoleClient)
	loaded.File.RPC = fakeRPC.Config()
	loaded.File.RPC.URL = gate.URL

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	err = main.WaitForChainAccess(ctx, loaded, rpc.New(gate.URL), 10*time.Millisecond)
	require.Error(t, err, "a check that fails once the rpc answers must stop the daemon")
	require.ErrorIs(t, err, preflight.ErrPreflightFailed)
	require.NotErrorIs(t, err, preflight.ErrRPCUnreachable)
	require.EqualValues(t, 2, refused.Load(), "it retried while the rpc was down")
}

func TestWaitForChainAccess_ReturnsWhenTheDaemonStops(t *testing.T) {
	// Not parallel: unreachableLoaded pins the build identity (version.ProgramID/Mint, package
	// globals), which other tests here read through version.Current() (-race on the stack).

	loaded := unreachableLoaded(t, config.RoleClient)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	require.NoError(t, main.WaitForChainAccess(ctx, loaded, rpc.New(loaded.File.RPC.URL), 10*time.Millisecond))
}
