//go:build !windows

package preflight

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// replaceableTool puts an executable in a folder any account can change, and only that folder on PATH.
func replaceableTool(t *testing.T, name string) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"), 0o755)) //#nosec G306 -- test tool
	require.NoError(t, os.Chmod(dir, 0o777))                                                 //#nosec G302 -- the folder must be replaceable
	t.Setenv("PATH", dir)

	return dir
}

// A tool in a folder other accounts can change is reported as found and refused, with the path and
// how to install it, not as missing.
func TestRefusedBinary_NamesThePathAndTheFix(t *testing.T) {
	dir := replaceableTool(t, "dvpn-refused-tool")

	_, err := service.LookPath("dvpn-refused-tool")
	require.ErrorIs(t, err, service.ErrUntrustedBinary)

	msg := refusedBinary("dvpn-refused-tool", err, "linux")
	require.True(t, strings.HasPrefix(msg, "found dvpn-refused-tool but refused it ("), msg)
	require.Contains(t, msg, dir, "the message names where the tool was found")
	require.Contains(t, msg, "owned by root, mode 0755")
	require.Contains(t, msg, "/usr/local/bin")
	require.NotContains(t, msg, "missing")
}

func TestBinariesProblem_RefusedIsNotMissing(t *testing.T) {
	replaceableTool(t, "dvpn-refused-tool")

	got := binariesProblemOf([]string{"dvpn-refused-tool", "dvpn-absent-tool"})
	require.Contains(t, got, "missing on PATH: [dvpn-absent-tool]")
	require.Contains(t, got, "found dvpn-refused-tool but refused it")
	require.NotContains(t, got, "[dvpn-refused-tool")
	require.Empty(t, binariesProblemOf(nil))
}

func TestTrustedInstallHint_PerOS(t *testing.T) {
	t.Parallel()

	require.Contains(t, trustedInstallHint("linux"), "/usr/local/bin")
	require.Contains(t, trustedInstallHint("darwin"), "owned by root")
	require.Contains(t, trustedInstallHint("windows"), "Program Files")
}
