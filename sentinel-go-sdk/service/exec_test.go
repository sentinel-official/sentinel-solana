package service_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

type fakeExiter struct {
	done chan struct{}
}

func (p *fakeExiter) Done() <-chan struct{} { return p.done }

func (p *fakeExiter) Err() error { return fmt.Errorf("%w: boom", service.ErrProcessExited) }

func TestEnvironWithout(t *testing.T) {
	t.Parallel()

	env := []string{"KEEP=1", "Drop=1", "DROP=1", "=C:=C:\\", "DROPPED=1"}
	names := []string{"DROP"}

	require.Equal(t, []string{"KEEP=1", "Drop=1", "=C:=C:\\", "DROPPED=1"}, service.EnvironWithout(env, names, "linux"))
	require.Equal(t, []string{"KEEP=1", "=C:=C:\\", "DROPPED=1"}, service.EnvironWithout(env, names, goosWindows))
}

func TestKnownBinaryDirs(t *testing.T) {
	programFiles := `C:\Program Files`
	t.Setenv("ProgramFiles", programFiles)

	cases := map[string][]string{
		"darwin": {
			"/opt/homebrew/bin",
			"/opt/homebrew/sbin",
			"/usr/local/bin",
			"/usr/local/sbin",
			"/usr/sbin",
			"/sbin",
		},
		"linux":     nil,
		goosWindows: nil,
	}
	for goos, want := range cases {
		require.Equal(t, want, service.KnownBinaryDirs(goos), goos)
	}
}

func TestLookPath_FindsOnPath(t *testing.T) {
	dir := t.TempDir()
	path := writeExecutable(t, dir, binaryName("fakebin"))
	t.Setenv("PATH", dir)

	got, err := service.LookPath("fakebin")
	require.NoError(t, err)
	require.Equal(t, path, got)
}

func TestLookPath_KeepsExplicitPaths(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	explicit := writeExecutable(t, t.TempDir(), binaryName("tool"))

	got, err := service.LookPath(explicit)
	require.NoError(t, err)
	require.Equal(t, explicit, got)

	_, err = service.LookPath(filepath.Join(t.TempDir(), "missing-tool"))
	require.ErrorIs(t, err, service.ErrBinaryNotFound)
}

func TestLookPath_Missing(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	_, err := service.LookPath("no-such-binary")
	require.ErrorIs(t, err, service.ErrBinaryNotFound)
	require.EqualError(t, err, "binary not found: no-such-binary")
}

func TestLookPath_RegisteredDirs(t *testing.T) {
	dir := t.TempDir()
	driver := fakeClientDriver(1 << 25)
	driver.LookupDirs = func() []string { return []string{dir} }
	service.RegisterClient(service.Name("fake-dirs"), driver)
	t.Cleanup(func() { service.DeregisterForTest(service.Name("fake-dirs")) })

	path := writeExecutable(t, dir, binaryName("fakebin"))
	t.Setenv("PATH", t.TempDir())

	got, err := service.LookPath("fakebin")
	require.NoError(t, err)
	require.Equal(t, path, got)
}

func TestPollReady(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		calls := 0
		probe := func(context.Context) error {
			calls++
			if calls < 3 {
				return errors.New("not yet")
			}

			return nil
		}

		require.NoError(t, service.PollReady(t.Context(), probe))
		require.Equal(t, 3, calls)
	})
}

func TestPollReady_ElapsedCallerDeadline(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"without process", "with process"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithDeadline(t.Context(), time.Time{})
			defer cancel()

			calls := 0
			probe := func(context.Context) error {
				calls++

				return errors.New("called after deadline")
			}

			var err error

			if name == "with process" {
				p := &fakeExiter{done: make(chan struct{})}
				err = service.PollReadyProcess(ctx, p, probe)
			} else {
				err = service.PollReady(ctx, probe)
			}

			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Zero(t, calls)
		})
	}
}

func TestPollReady_Timeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		probe := func(context.Context) error { return errors.New("never") }

		err := service.PollReady(t.Context(), probe)
		require.ErrorIs(t, err, service.ErrProcessNotReady)
		require.Contains(t, err.Error(), "never")
	})
}

func TestRun_UnsetsEnvironment(t *testing.T) {
	if runtime.GOOS == goosWindows {
		t.Skip("requires the env binary")
	}

	t.Setenv("SENTINEL_TEST_DROP", "1")
	t.Setenv("SENTINEL_TEST_KEEP", "1")

	out, err := service.Run(t.Context(), service.Command{Path: "env", Unset: []string{"SENTINEL_TEST_DROP"}})
	require.NoError(t, err)
	require.NotContains(t, string(out), "SENTINEL_TEST_DROP=")
	require.Contains(t, string(out), "SENTINEL_TEST_KEEP=1")
}

func writeExecutable(t *testing.T, dir, name string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700)) //nolint:gosec

	return path
}
