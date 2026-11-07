package client_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
)

func TestRunCommand_FeedsStdin(t *testing.T) {
	// Re-exec the test binary as a helper that echoes stdin (TestMain, SENTINEL_STDIN_ECHO), so stdin
	// feeding is tested on every OS rather than relying on the unix-only `cat`.
	binary, err := os.Executable()
	require.NoError(t, err)

	t.Setenv("SENTINEL_STDIN_ECHO", "1")

	out, err := client.RunCommand(t.Context(), "hello\n", binary)
	require.NoError(t, err)
	require.Equal(t, "hello\n", string(out))
}

func TestRunCommand_MissingBinary(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	_, err := client.RunCommand(t.Context(), "", "no-such-tool")
	require.Error(t, err)
}
