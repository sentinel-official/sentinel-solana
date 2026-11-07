package client_test

import (
	"io"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if os.Getenv("SENTINEL_PF_COMMAND") == "1" {
		os.Exit(fakePFCommand())
	}

	// Helper process for TestRunCommand_FeedsStdin: copy stdin to stdout and exit, so stdin feeding
	// is exercised with the test binary itself (works on every OS) instead of a unix-only `cat`.
	if os.Getenv("SENTINEL_STDIN_ECHO") == "1" {
		_, _ = io.Copy(os.Stdout, os.Stdin)
		os.Exit(0)
	}

	os.Exit(m.Run())
}
