package cli_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	cli "github.com/sentinel-official/sentinel-dvpn/internal/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
)

func TestDialerTunnelIface(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dir := filepath.Join(root, "c0")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, instance.TunnelIfaceFileName), []byte("utun7\n"), 0o600))

	require.Equal(t, "utun7", cli.DialerTunnelIface(root, instance.Tag("c0"))) // recorded name, trimmed
	require.Equal(t, "", cli.DialerTunnelIface(root, instance.Tag("c1")))      // no marker -> fall back
}
