package client_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/leftover"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

func TestFirewall_RPCAndWSAddrsColumn(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	_ = newRuntime(t, home, home.ProgramClient)

	require.True(t, home.DB.Migrator().HasColumn(&client.Firewall{}, "rpc_and_ws_addrs"))
}

func TestRecover_EmptyNodeAddrs_Fails(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	firewall := &recordingFirewallBackend{}

	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, firewall)

	row := client.Firewall{
		Iface:         "dvpn-c0",
		NodeURI:       "https://127.0.0.1",
		Persistent:    true,
		RPCAndWSAddrs: "127.0.0.1:8899",
	}
	require.NoError(t, home.DB.WithContext(t.Context()).Create(&row).Error)

	err := rt.Recover(t.Context())
	require.ErrorIs(t, err, client.ErrFirewallRowIncomplete)
	require.Empty(t, firewall.applies())
}

func TestRecover_EmptyRPCAndWSAddrs_Fails(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	firewall := &recordingFirewallBackend{}

	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, firewall)

	row := client.Firewall{
		Iface:      "dvpn-c0",
		NodeAddrs:  "127.0.0.1",
		NodeURI:    "https://127.0.0.1",
		Persistent: true,
	}
	require.NoError(t, home.DB.WithContext(t.Context()).Create(&row).Error)

	err := rt.Recover(t.Context())
	require.ErrorIs(t, err, client.ErrFirewallRowIncomplete)
	require.Empty(t, firewall.applies())
}

func TestRuntime_Recover_CleansEveryProtocol(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	remover := &testutil.RecordingInterfaceRemover{}
	client.SetInterfaceRemover(rt, remover)
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})
	registerFakeClientOnce()

	var dirs []string

	cleanup := func(dir string) { dirs = append(dirs, dir) }
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{
		hooks: clientHooks{onCleanup: &cleanup},
		state: sdktest.NewFakeState(sdktest.FakeName),
	})

	require.NoError(t, rt.Recover(t.Context()))
	require.Equal(t, []string{home.Loaded.ServiceDir(sdktest.FakeName)}, dirs)
	require.Contains(t, defaultCleanups.cleanups(), home.Loaded.ServiceDir(otherFakeName))
	require.Equal(t, []string{home.Loaded.Tag.Interface()}, remover.Names)
}

func TestRuntime_Recover_ClosesRemoteSession(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})

	row := client.Session{
		ID:          home.NodeService.SessionIDs()[0],
		NodeAddress: home.NodeAddress.String(),
		Status:      client.SessionStatusActive,
	}
	require.NoError(t, home.DB.WithContext(t.Context()).Create(&row).Error)

	require.NoError(t, rt.Recover(t.Context()))
	require.Eventually(t, func() bool { return home.NodeService.ClosedCount() == 1 }, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, row.ID, home.NodeService.ClosedSessionID(0))
}

func TestRuntime_Recover_ConfigSwitchRestoreFailure(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	path := filepath.Join(t.TempDir(), "resolv.conf")
	require.NoError(t, os.Mkdir(path, 0o700))
	backup := filepath.Join(t.TempDir(), "resolv.conf.bak")
	require.NoError(t, os.WriteFile(backup, []byte("nameserver 192.0.2.53\n"), 0o600))
	client.SetDNSBackend(rt, client.NewLinuxDNSBackend(config.DNSNone, path, backup))
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})

	err := rt.Recover(t.Context())
	require.ErrorIs(t, err, client.ErrDNSCleanupFailed)
	require.False(t, client.Recovered(rt))

	got, err := os.ReadFile(backup) //#nosec G304
	require.NoError(t, err)
	require.Equal(t, "nameserver 192.0.2.53\n", string(got))
}

func TestRuntime_Recover_DNSCleanupFailure(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	cleanupErr := errors.New("restore failed")
	backend := &recordingDNSBackend{cleanupErr: cleanupErr}
	client.SetDNSBackend(rt, backend)
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})

	err := rt.Recover(t.Context())
	require.ErrorIs(t, err, client.ErrDNSCleanupFailed)
	require.ErrorIs(t, err, cleanupErr)
	require.Equal(t, 1, backend.cleanupCount())
	require.False(t, client.Recovered(rt))
}

func TestRuntime_Recover_MarksStaleSessionsUnclean(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	backend := &recordingDNSBackend{}
	client.SetDNSBackend(rt, backend)
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})

	row := client.Session{
		Clean:  true,
		ID:     "stale-session",
		Status: client.SessionStatusActive,
	}
	require.NoError(t, home.DB.WithContext(t.Context()).Create(&row).Error)

	require.NoError(t, rt.Recover(t.Context()))
	require.Equal(t, 1, backend.cleanupCount())
	require.True(t, client.Recovered(rt))
	require.NoError(t, home.DB.WithContext(t.Context()).First(&row, "id = ?", row.ID).Error)
	require.False(t, row.Clean)
	require.Equal(t, "daemon restarted", row.Error)
	require.Equal(t, client.SessionStatusEnded, row.Status)
	require.Positive(t, row.EndedUnix)
}

func TestRuntime_Recover_RemovesLeftoverCopies(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})
	registerFakeClientOnce()

	toolDir := home.Loaded.Placement(sdktest.FakeName).ToolDir
	base := home.Loaded.Tag.Interface()
	marked := "# " + service.ManagedMarker + "\n"
	leftoverCopy := filepath.Join(toolDir, base+".conf")
	otherTag := filepath.Join(toolDir, base+"0.conf")
	unmarked := filepath.Join(toolDir, base+".json")

	for path, content := range map[string]string{leftoverCopy: marked, otherTag: marked, unmarked: "{}\n"} {
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	}

	require.NoError(t, rt.Recover(t.Context()))
	require.NoFileExists(t, leftoverCopy)
	require.FileExists(t, otherTag)
	require.FileExists(t, unmarked)
}

func TestRuntime_Recover_ReportsLeftoverInterface(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})
	client.SetInterfaceRemover(rt, &testutil.RecordingInterfaceRemover{Err: leftover.ErrInterfaceExists})

	err := rt.Recover(t.Context())
	require.ErrorIs(t, err, client.ErrServiceCleanupFailed)
	require.ErrorIs(t, err, leftover.ErrInterfaceExists)
	require.False(t, client.Recovered(rt))
}

func TestRuntime_Recover_ReportsServiceCleanupFailure(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)

	registerFakeClientOnce()

	failure := errors.New("tunnel cleanup failed")
	cleanupError := func() error { return failure }
	rt := newRuntime(t, home, home.ProgramClient)
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{
		hooks: clientHooks{onCleanupError: &cleanupError},
		state: sdktest.NewFakeState(sdktest.FakeName),
	})
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})
	require.NoError(t, home.DB.WithContext(t.Context()).Create(&client.Session{
		ID:      "stale-local-cleanup",
		Service: sdktest.FakeName.String(),
		Status:  client.SessionStatusActive,
	}).Error)

	err := rt.Recover(t.Context())
	require.ErrorIs(t, err, client.ErrServiceCleanupFailed)
	require.ErrorIs(t, err, failure)
	require.False(t, client.Recovered(rt))
}

func TestRuntime_Recover_ReportsStaleSessionListFailure(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})
	require.NoError(t, home.DB.Migrator().DropTable(&client.Session{}))

	require.ErrorIs(t, rt.Recover(t.Context()), client.ErrServiceCleanupFailed)
	require.False(t, client.Recovered(rt))
}

func TestRuntime_Recover_RetriesFailedLocalCleanupAfterRestart(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)

	registerFakeClientOnce()

	failure := errors.New("tunnel cleanup failed")
	fail := true
	calls := 0
	cleanupError := func() error {
		calls++

		if fail {
			return failure
		}

		return nil
	}
	cfg := fakeClientConfig{
		hooks: clientHooks{onCleanupError: &cleanupError},
		state: sdktest.NewFakeState(sdktest.FakeName),
	}
	row := client.Session{
		ID:      "stale-local-cleanup",
		Service: sdktest.FakeName.String(),
		Status:  client.SessionStatusActive,
	}

	rt := newRuntime(t, home, home.ProgramClient)
	client.SetService(rt, sdktest.FakeName, cfg)
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})
	require.NoError(t, home.DB.WithContext(t.Context()).Create(&row).Error)
	require.ErrorIs(t, rt.Recover(t.Context()), failure)
	require.False(t, client.Recovered(rt))
	require.NoError(t, home.DB.WithContext(t.Context()).First(&row, "id = ?", row.ID).Error)
	require.Equal(t, client.SessionStatusEnded, row.Status)

	fail = false
	restarted := newRuntime(t, home, home.ProgramClient)
	client.SetService(restarted, sdktest.FakeName, cfg)
	client.SetDNSBackend(restarted, &recordingDNSBackend{})
	client.SetFirewallBackend(restarted, &recordingFirewallBackend{})
	require.NoError(t, restarted.Recover(t.Context()))
	require.Equal(t, 2, calls)
	require.True(t, client.Recovered(restarted))
}

func TestRuntime_StartSession_IncompleteFirewallRowFails(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	firewall := &recordingFirewallBackend{}

	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, firewall)

	row := client.Firewall{
		Iface:     "dvpn-c0",
		NodeAddrs: "127.0.0.1",
		RPCURL:    home.Loaded.File.RPC.URL,
		WSURL:     home.Loaded.File.RPC.WSURL,
	}
	require.NoError(t, home.DB.WithContext(t.Context()).Create(&row).Error)

	registerFakeClientOnce()
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{state: sdktest.NewFakeState(sdktest.FakeName)})

	req := seedTicketRequest(t, fakeRPC, home, home.DaemonKey.PublicKey(), time.Now().Add(time.Hour).Unix())

	_, err := client.StartSession(t.Context(), rt, req)
	require.ErrorIs(t, err, client.ErrFirewallRowIncomplete)
	require.Empty(t, firewall.applies())
}

func TestRuntime_Recover_RemovesRecordedLiveInterface(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, home.ProgramClient)
	remover := &testutil.RecordingInterfaceRemover{}
	client.SetInterfaceRemover(rt, remover)
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})

	// A hard kill left a marker naming a kernel-assigned interface (a macOS utun) that is not the
	// tag's dvpn-<instance> name. Recovery must remove that interface too, so the CLI's promise —
	// "start the service to recover and remove it" — holds for utun / OpenVPN, not only WireGuard.
	marker := filepath.Join(home.Loaded.Dir, instance.TunnelIfaceFileName)
	require.NoError(t, os.WriteFile(marker, []byte("utun7\n"), 0o600))

	require.NoError(t, rt.Recover(t.Context()))

	require.Contains(t, remover.Names, "utun7")
	require.Contains(t, remover.Names, home.Loaded.Tag.Interface())

	_, err := os.Stat(marker)
	require.True(t, os.IsNotExist(err)) // marker cleared on recovery
}

func TestRuntime_Recover_CopyRemovalBestEffortLiveCleanupFatal(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *client.Runtime {
		t.Helper()

		fakeRPC := sdktest.NewFakeRPC(t)
		home := testutil.NewClientHome(t, fakeRPC)
		rt := newRuntime(t, home, home.ProgramClient)
		client.SetInterfaceRemover(rt, &testutil.RecordingInterfaceRemover{})
		client.SetDNSBackend(rt, &recordingDNSBackend{})
		client.SetFirewallBackend(rt, &recordingFirewallBackend{})
		registerFakeClientOnce()

		return rt
	}

	t.Run("leftover copy removal failure is not fatal", func(t *testing.T) {
		t.Parallel()

		rt := setup(t)
		// The service's own (live) cleanup succeeds; only removing a stale tool-dir file copy fails,
		// as a non-root run hits on a root-owned copy from a prior privileged run. That copy is an old
		// artifact, not a live leak, so recovery must still succeed.
		cleanup := func(string) {}
		client.SetService(rt, sdktest.FakeName, fakeClientConfig{
			hooks: clientHooks{onCleanup: &cleanup},
			state: sdktest.NewFakeState(sdktest.FakeName),
		})
		client.SetRemoveCopies(rt, func([]string, string) error { return errors.New("permission denied") })

		require.NoError(t, rt.Recover(t.Context()))
		require.True(t, client.Recovered(rt))
	})

	t.Run("live service cleanup failure stays fatal", func(t *testing.T) {
		t.Parallel()

		rt := setup(t)
		// A failure to tear down the previous run's live state (stale tunnel process / interface /
		// adapter) must abort start, or the daemon comes up alongside a live stale tunnel.
		boom := func() error { return errors.New("stopping dvpn-c0: operation not permitted") }
		client.SetService(rt, sdktest.FakeName, fakeClientConfig{
			hooks: clientHooks{onCleanupError: &boom},
			state: sdktest.NewFakeState(sdktest.FakeName),
		})

		err := rt.Recover(t.Context())
		require.ErrorIs(t, err, client.ErrServiceCleanupFailed)
		require.False(t, client.Recovered(rt))
	})
}
