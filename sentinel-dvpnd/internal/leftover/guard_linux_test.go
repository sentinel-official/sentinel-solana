//go:build linux

package leftover_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/leftover"
)

const (
	guardRoleEnv   = "LEFTOVER_GUARD_TEST_ROLE"
	guardRecordEnv = "LEFTOVER_GUARD_TEST_RECORD"
	guardParentEnv = "LEFTOVER_GUARD_TEST_PARENT"
	guardReadyLine = "guard-armed"
)

type fileRemover struct{ path string }

func (r fileRemover) RemoveInterface(_ context.Context, name string) error {
	f, err := os.OpenFile(r.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //#nosec G304 -- test file
	if err != nil {
		return err
	}

	if _, err := f.WriteString(name + "\n"); err != nil {
		return errorsJoin(err, f.Close())
	}

	return f.Close()
}

func errorsJoin(a, b error) error {
	if a != nil {
		return a
	}

	return b
}

// TestGuardHelperProcess is not a test: the cases below re-run the test binary as a fake daemon (role
// "daemon") that starts a guard child (role "guard") with StartGuardProcess, exactly as dvpnd does.
func TestGuardHelperProcess(t *testing.T) {
	switch os.Getenv(guardRoleEnv) {
	case "daemon":
		release, err := leftover.StartGuardProcess(
			[]string{os.Args[0], "-test.run=^TestGuardHelperProcess$"},
			[]string{guardRoleEnv + "=guard", guardParentEnv + "=" + strconv.Itoa(os.Getpid())},
		)
		if err != nil {
			os.Exit(2)
		}

		time.Sleep(300 * time.Millisecond) // the guard arms its signal handler
		os.Stdout.WriteString(guardReadyLine + "\n")

		if os.Getenv("LEFTOVER_GUARD_TEST_CLEAN") == "1" {
			if err := release(); err != nil {
				os.Exit(3)
			}

			os.Exit(0)
		}

		select {} // until SIGKILL
	case "guard":
		parent, err := strconv.Atoi(os.Getenv(guardParentEnv))
		if err != nil {
			os.Exit(4)
		}

		g := leftover.Guard{
			Log:     zerolog.Nop(),
			Names:   func() []string { return []string{"dvpn-c0", "utun9", "dvpn-c0"} },
			Remover: fileRemover{path: os.Getenv(guardRecordEnv)},
		}
		leftover.RunGuard(context.Background(), parent, g)
		os.Exit(0)
	default:
		t.Skip("helper process")
	}
}

func startFakeDaemon(t *testing.T, record string, clean bool) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestGuardHelperProcess$") //#nosec G204 -- the test binary
	cmd.Env = append(os.Environ(), guardRoleEnv+"=daemon", guardRecordEnv+"="+record)

	if clean {
		cmd.Env = append(cmd.Env, "LEFTOVER_GUARD_TEST_CLEAN=1")
	}

	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	buf := make([]byte, 64)
	n, err := out.Read(buf)
	require.NoError(t, err)
	require.Contains(t, string(buf[:n]), guardReadyLine)

	return cmd
}

func readRecord(path string) []string {
	raw, err := os.ReadFile(path) //#nosec G304 -- test file
	if err != nil {
		return nil
	}

	return strings.Fields(string(raw))
}

// A SIGKILLed daemon left its kernel tunnel up (~8 s of traffic to the node while status read no session,
// until the next start removed it). The guard child now removes it the moment the daemon dies.
func TestGuard_RemovesTheTunnelWhenTheDaemonIsKilled(t *testing.T) {
	t.Parallel()

	record := filepath.Join(t.TempDir(), "removed")
	daemon := startFakeDaemon(t, record, false)

	require.NoError(t, daemon.Process.Signal(syscall.SIGKILL))
	_ = daemon.Wait() //nolint:errcheck // killed on purpose

	require.Eventually(t, func() bool { return len(readRecord(record)) == 2 }, 3*time.Second, 20*time.Millisecond,
		"the guard removes the tunnel interfaces once the daemon is killed")
	require.Equal(t, []string{"dvpn-c0", "utun9"}, readRecord(record))
}

func TestGuard_LeavesEverythingWhenTheDaemonStopsCleanly(t *testing.T) {
	t.Parallel()

	record := filepath.Join(t.TempDir(), "removed")
	daemon := startFakeDaemon(t, record, true)
	require.NoError(t, daemon.Wait(), "release waits for the guard to exit")

	time.Sleep(300 * time.Millisecond)
	require.Empty(t, readRecord(record), "a clean stop already tore the tunnel down; the guard touches nothing")
}

func TestGuard_Watch(t *testing.T) {
	t.Parallel()

	removed := &recordingRemover{}
	g := leftover.Guard{Log: zerolog.Nop(), Names: func() []string { return []string{"dvpn-c0", ""} }, Remover: removed}

	sigs := make(chan os.Signal, 1)
	sigs <- syscall.SIGTERM // not the guard's signal: ignored
	go func() { time.Sleep(50 * time.Millisecond); sigs <- leftover.GuardDied }()

	require.True(t, g.Watch(t.Context(), sigs, leftover.GuardDied, leftover.GuardRelease))
	require.Equal(t, []string{"dvpn-c0"}, removed.names)

	removed.names = nil
	sigs <- leftover.GuardRelease
	require.False(t, g.Watch(t.Context(), sigs, leftover.GuardDied, leftover.GuardRelease))
	require.Empty(t, removed.names)
}

type recordingRemover struct{ names []string }

func (r *recordingRemover) RemoveInterface(_ context.Context, name string) error {
	r.names = append(r.names, name)

	return nil
}
