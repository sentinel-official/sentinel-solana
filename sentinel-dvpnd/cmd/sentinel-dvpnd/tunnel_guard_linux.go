//go:build linux && !android

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/leftover"
)

// startTunnelGuard starts the guard child for a client daemon and returns its release func. A kernel
// WireGuard/AmneziaWG interface outlives a killed daemon; the guard removes it the moment the daemon dies.
func startTunnelGuard(loaded *config.Loaded) (func() error, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locating the daemon executable: %w", err)
	}

	argv := []string{
		self, tunnelGuardUse,
		"--iface", loaded.Tag.Interface(),
		"--parent", strconv.Itoa(os.Getpid()),
	}

	return leftover.StartGuardProcess(argv, nil)
}

func runTunnelGuard(ctx context.Context, opts tunnelGuardOptions, out io.Writer) error {
	if opts.parent <= 0 {
		return fmt.Errorf("%s: --parent is required", tunnelGuardUse)
	}

	g := leftover.Guard{
		Log:     zerolog.New(out).With().Timestamp().Str("role", "client").Logger(),
		Names:   func() []string { return opts.ifaces },
		Remover: leftover.NewRemover(),
	}
	leftover.RunGuard(ctx, opts.parent, g)

	return nil
}
