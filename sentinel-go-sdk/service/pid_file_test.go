package service_test

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

func TestKillPIDFile_IgnoresConfigPrefixCollision(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("process signaling is disabled on Windows")
	}

	config := filepath.Join(t.TempDir(), "server.conf")
	cmd := exec.CommandContext(
		t.Context(), "sh", "-c", "trap 'kill \"$child\"' TERM; sleep 30 & child=$!; echo ready; wait \"$child\"",
		"fixture", "--config", config+".foreign",
	)
	ready, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	_, err = bufio.NewReader(ready).ReadString('\n')
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		_ = cmd.Wait()

		close(done)
	}()

	t.Cleanup(func() {
		_ = cmd.Process.Kill()

		<-done
	})

	path := filepath.Join(t.TempDir(), "client.pid")
	require.NoError(t, service.WritePIDFile(path, cmd.Process.Pid))
	require.NoError(t, service.KillPIDFile(t.Context(), path, "sh", "--config", config))

	select {
	case <-done:
		t.Fatal("config prefix collision terminated foreign process")
	default:
	}
}

func TestKillPIDFile_IgnoresForeignProcess(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("process signaling is disabled on Windows")
	}

	cmd := exec.CommandContext(t.Context(), "sleep", "30")
	require.NoError(t, cmd.Start())

	done := make(chan struct{})

	go func() {
		_ = cmd.Wait()

		close(done)
	}()

	t.Cleanup(func() {
		_ = cmd.Process.Kill()

		<-done
	})

	path := filepath.Join(t.TempDir(), "client.pid")

	for _, binary := range []string{"sh", "sentinel-missing-tool"} {
		require.NoError(t, service.WritePIDFile(path, cmd.Process.Pid))
		require.NoError(t, service.KillPIDFile(t.Context(), path, binary, "--config", "sentinel-foreign-marker"))
		require.NoFileExists(t, path)
	}

	select {
	case <-done:
		t.Fatal("foreign process exited")
	default:
	}
}

func TestKillPIDFile_IgnoresUnsafePID(t *testing.T) {
	t.Parallel()

	for _, pid := range []string{"0", "1", "-1", "invalid"} {
		t.Run(pid, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "client.pid")
			require.NoError(t, os.WriteFile(path, []byte(pid), 0o600))

			require.NoError(t, service.KillPIDFile(t.Context(), path, "sh", "--config", "sentinel-test"))
			require.NoFileExists(t, path)
		})
	}
}

func TestKillPIDFile_KillsMatchingProcess(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("process signaling is disabled on Windows")
	}

	marker := "sentinel-pidfile-test-" + filepath.Base(t.TempDir())
	command := "trap 'kill \"$child\"' TERM; sleep 30 & child=$!; echo ready; wait \"$child\""
	cmd := exec.CommandContext(t.Context(), "sh", "-c", command, "fixture", "--config", marker)
	ready, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	line, err := bufio.NewReader(ready).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)

	done := make(chan struct{})

	go func() {
		_ = cmd.Wait()

		close(done)
	}()

	t.Cleanup(func() {
		_ = cmd.Process.Kill()

		<-done
	})

	path := filepath.Join(t.TempDir(), "client.pid")
	require.NoError(t, service.WritePIDFile(path, cmd.Process.Pid))

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	require.NoError(t, service.KillPIDFile(ctx, path, "sh", "--config", marker))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("matching process did not exit")
	}

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

//nolint:paralleltest // swaps the package-level image killer
func TestKillPIDFile_OwnedImageOnWindows(t *testing.T) {
	managed := filepath.Join(t.TempDir(), "openvpn.exe")
	killErr := errors.New("access denied")
	cases := []struct {
		err     error
		image   string
		name    string
		owned   bool
		removed bool
	}{
		{image: managed, name: "owned and killed", owned: true, removed: true},
		{image: filepath.Join(t.TempDir(), "openvpn.exe"), name: "not owned", removed: true},
		{err: killErr, image: managed, name: "kill error", owned: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "openvpn.pid")
			pid := os.Getpid() + 1
			require.NoError(t, service.WritePIDFile(path, pid))

			var owned bool

			previous := service.SetKillOwnedImage(func(_ context.Context, got int, owns func(string, time.Time) bool) error {
				require.Equal(t, pid, got)

				owned = owns(tc.image, time.Now())

				return tc.err
			})

			t.Cleanup(func() { service.SetKillOwnedImage(previous) })

			err := service.KillPIDFileFor(t.Context(), path, managed, "--config", "dvpn-c0.conf", "windows")
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.owned, owned)

			if tc.removed {
				require.NoFileExists(t, path)
			} else {
				require.FileExists(t, path)
			}
		})
	}
}

func TestKillPIDFile_RejectsIncompleteIdentity(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "client.pid")
	require.NoError(t, service.WritePIDFile(path, os.Getpid()+1))

	err := service.KillPIDFileFor(t.Context(), path, "", "", "", "linux")
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.FileExists(t, path)
}

func TestKillPIDFile_RetainsOwnedPIDOnCancelThenKills(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("process signaling is disabled on Windows")
	}

	marker := "sentinel-pidfile-ignore-term-" + filepath.Base(t.TempDir())
	input, writer, err := os.Pipe()

	require.NoError(t, err)
	defer func() { require.NoError(t, input.Close()) }()
	defer func() { require.NoError(t, writer.Close()) }()

	cmd := exec.CommandContext(
		t.Context(), "sh", "-c", "trap '' TERM; echo ready; read line", "fixture", "--config", marker,
	)
	cmd.Stdin = input
	ready, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	_, err = bufio.NewReader(ready).ReadString('\n')
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		_ = cmd.Wait()

		close(done)
	}()

	t.Cleanup(func() {
		_ = cmd.Process.Kill()

		<-done
	})

	path := filepath.Join(t.TempDir(), "client.pid")
	require.NoError(t, service.WritePIDFile(path, cmd.Process.Pid))

	shortCtx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, service.KillPIDFile(shortCtx, path, "sh", "--config", marker), context.DeadlineExceeded)
	_, err = os.Stat(path)
	require.NoError(t, err)

	select {
	case <-done:
		t.Fatal("TERM-ignoring process exited before KILL")
	default:
	}

	longCtx, stop := context.WithTimeout(t.Context(), 7*time.Second)
	defer stop()

	require.NoError(t, service.KillPIDFile(longCtx, path, "sh", "--config", marker))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("TERM-ignoring process survived KILL")
	}

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

//nolint:paralleltest // swaps the package-level process reader
func TestKillPIDFile_TreatsInvalidDeadPIDAsGone(t *testing.T) {
	if runtime.GOOS == goosWindows {
		t.Skip("process signaling is disabled on Windows")
	}

	cmd := exec.CommandContext(t.Context(), "true")
	require.NoError(t, cmd.Run())

	path := filepath.Join(t.TempDir(), "client.pid")
	require.NoError(t, service.WritePIDFile(path, cmd.Process.Pid))

	previous := service.SetReadProcessArgs(func(context.Context, int) (string, []string, error) {
		return "", nil, syscall.EINVAL
	})

	t.Cleanup(func() { service.SetReadProcessArgs(previous) })

	require.NoError(t, service.KillPIDFile(t.Context(), path, "sh", "--config", "sentinel-test"))
	require.NoFileExists(t, path)
}

func TestOwnsImage(t *testing.T) {
	t.Parallel()

	managed := "/tools/openvpn.exe"
	written := time.Unix(1_700_000_000, 0)
	cases := []struct {
		binary  string
		image   string
		name    string
		started time.Time
		want    bool
	}{
		{binary: managed, image: "/Tools/./OpenVPN.exe", name: "same path in other case", started: written, want: true},
		{binary: managed, image: "/other/openvpn.exe", name: "other path", started: written},
		{binary: managed, image: managed, name: "started after pid file", started: written.Add(time.Minute)},
		{binary: "sentinel-missing-tool", image: "sentinel-missing-tool", name: "unresolved binary", started: written},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, service.OwnsImage(tc.image, tc.binary, tc.started, written))
		})
	}
}

func TestWritePIDFile_RejectsUnsafePID(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "client.pid")

	require.ErrorIs(t, service.WritePIDFile(path, 1), validate.ErrInvalidConfig)
	require.NoFileExists(t, path)
}

func TestWritePIDFile_WritesProcessPID(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("shell process fixture requires Unix")
	}

	process, err := service.StartProcess(t.Context(), service.Command{
		Args: []string{"-c", "exec sleep 30"},
		Path: "sh",
	}, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = process.Stop(context.Background()) })
	require.Greater(t, process.PID(), 1)

	path := filepath.Join(t.TempDir(), "client.pid")
	require.NoError(t, service.WritePIDFile(path, process.PID()))
	raw, err := os.ReadFile(path) //#nosec G304
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(process.PID())+"\n", string(raw))
}

// startWrapped starts a process the way a wrapper script leaves it: argv[0] and the executable are
// a file whose name is not the managed binary's.
func startWrapped(t *testing.T, marker string) (*exec.Cmd, string, chan struct{}) {
	t.Helper()

	sh, err := exec.LookPath("sh")
	require.NoError(t, err)
	real, err := filepath.EvalSymlinks(sh)
	require.NoError(t, err)

	command := "trap 'kill \"$child\"' TERM; sleep 30 & child=$!; echo ready; wait \"$child\""
	cmd := exec.CommandContext(t.Context(), real, "-c", command, "fixture", "--config", marker)
	ready, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	line, err := bufio.NewReader(ready).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)

	done := make(chan struct{})

	go func() {
		_ = cmd.Wait()

		close(done)
	}()

	t.Cleanup(func() {
		_ = cmd.Process.Kill()

		<-done
	})

	return cmd, real, done
}

func TestKillPIDFile_KillsProcessBehindWrapper(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "linux" {
		t.Skip("reads /proc")
	}

	marker := "sentinel-wrapper-test-" + filepath.Base(t.TempDir())
	cmd, real, done := startWrapped(t, marker)

	path := filepath.Join(t.TempDir(), "openvpn.pid")

	// Without a record of what it runs, the process does not look like the managed binary.
	require.NoError(t, service.WritePIDFile(path, cmd.Process.Pid))
	require.NoError(t, service.KillPIDFile(t.Context(), path, "sentinel-wrapper-probe", "--config", marker))

	select {
	case <-done:
		t.Fatal("process killed without a recorded executable")
	default:
	}

	// A record naming another file of the same name is not enough either.
	require.NoError(t, service.WritePIDRecord(path, cmd.Process.Pid, filepath.Join(t.TempDir(), filepath.Base(real))))
	require.ErrorContains(t, service.KillPIDFile(t.Context(), path, "sentinel-wrapper-probe", "--config", marker),
		"resolving managed executable")

	select {
	case <-done:
		t.Fatal("process killed under a foreign executable record")
	default:
	}

	require.NoError(t, service.WritePIDRecord(path, cmd.Process.Pid, real))

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	require.NoError(t, service.KillPIDFile(ctx, path, "sentinel-wrapper-probe", "--config", marker))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recorded process did not exit")
	}

	require.NoFileExists(t, path)
}

func TestKillPIDFile_RecordNeedsMatchingConfig(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "linux" {
		t.Skip("reads /proc")
	}

	marker := "sentinel-wrapper-test-" + filepath.Base(t.TempDir())
	cmd, real, done := startWrapped(t, marker)

	path := filepath.Join(t.TempDir(), "openvpn.pid")
	require.NoError(t, service.WritePIDRecord(path, cmd.Process.Pid, real))
	require.NoError(t, service.KillPIDFile(t.Context(), path, "sentinel-wrapper-probe", "--config", marker+"-other"))

	select {
	case <-done:
		t.Fatal("process killed for another config")
	default:
	}
}

func TestWritePIDRecord_RejectsUnsafeExecutable(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "x.pid")
	for _, exe := range []string{"relative/bin", "/bin/a\n/bin/b"} {
		require.ErrorIs(t, service.WritePIDRecord(path, 4242, exe), validate.ErrInvalidConfig, exe)
	}
}
