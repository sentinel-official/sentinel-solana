//go:build unix

package daemon_test

import (
	"context"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/daemon"
)

func TestListen_Busy(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := daemon.Listen(t.Context(), path, "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	other, err := daemon.Listen(t.Context(), path, "")
	require.ErrorContains(t, err, "already listening")
	require.Nil(t, other)

	_, err = os.Stat(path)
	require.NoError(t, err)
}

func TestListen_Canceled(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "daemon.sock")
	data := []byte("existing socket path")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	listener, err := daemon.Listen(ctx, path, "")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, listener)

	got, err := os.ReadFile(filepath.Clean(path))
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestListen_Group(t *testing.T) {
	t.Parallel()

	group, err := user.LookupGroupId(strconv.Itoa(os.Getgid()))
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := daemon.Listen(t.Context(), path, group.Name)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o660), info.Mode().Perm())

	stat, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	require.EqualValues(t, os.Getgid(), stat.Gid)
}

func TestListen_ProbeError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "daemon.sock")
	require.NoError(t, os.Symlink(filepath.Base(path), path))

	listener, err := daemon.Listen(t.Context(), path, "")
	if listener != nil {
		t.Cleanup(func() { require.NoError(t, listener.Close()) })
	}

	require.ErrorIs(t, err, syscall.ELOOP)
	require.ErrorContains(t, err, "probing daemon socket")
	require.Nil(t, listener)

	target, err := os.Readlink(path)
	require.NoError(t, err)
	require.Equal(t, filepath.Base(path), target)
}

func TestListen_RecoverStaleSocket(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "daemon.sock")

	var lc net.ListenConfig

	stale, err := lc.Listen(t.Context(), "unix", path)
	require.NoError(t, err)
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	require.NoError(t, stale.Close())

	info, err := os.Lstat(path)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSocket)

	listener, err := daemon.Listen(t.Context(), path, "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	var dialer net.Dialer

	conn, err := dialer.DialContext(t.Context(), "unix", path)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

// A daemon started without its socket unit makes the socket folder (/run/sentinel-dvpn/<tag>).
func TestListen_CreatesSocketFolder(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "run", "c0", "daemon.sock")
	ln, err := daemon.Listen(t.Context(), path, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.ModeSocket, info.Mode().Type())
}
