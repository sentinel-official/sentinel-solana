package client

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
)

// The marker stores whatever name the OS assigned, whichever protocol is in use.
func TestTunnelIfaceMarker(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"dvpn-c0", "utun7", "tun0"} { // wireguard / macOS utun / openvpn
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, instance.TunnelIfaceFileName)

			writeTunnelIface(dir, name, zerolog.Nop())

			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, name+"\n", string(raw))

			if runtime.GOOS != "windows" {
				info, err := os.Stat(path)
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
			}

			// The atomic write leaves no temp file behind.
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, entries, 1)

			// A hard kill never runs teardown, so the marker survives for the CLI to read.
			// (Simulated by not calling removeTunnelIface here.) Then teardown clears it,
			removeTunnelIface(dir, zerolog.Nop())
			_, err = os.Stat(path)
			require.True(t, os.IsNotExist(err))

			// and a second clear (e.g. recovery after a graceful stop) is a no-op.
			removeTunnelIface(dir, zerolog.Nop())
		})
	}
}

func TestTunnelIfaceMarker_EmptyInputsWriteNothing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTunnelIface(dir, "", zerolog.Nop()) // no live interface -> nothing to warn about
	writeTunnelIface("", "utun7", zerolog.Nop())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}
