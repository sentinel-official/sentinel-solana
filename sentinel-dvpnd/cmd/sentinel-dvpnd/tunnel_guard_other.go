//go:build !linux || android

package main

import (
	"context"
	"errors"
	"io"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
)

// A user-space tunnel (Windows wintun, a macOS utun, Android's VpnService fd) closes with the daemon's
// process, so only Linux kernel interfaces need the guard.
func startTunnelGuard(*config.Loaded) (func() error, error) {
	return func() error { return nil }, nil
}

func runTunnelGuard(context.Context, tunnelGuardOptions, io.Writer) error {
	return errors.New("the tunnel guard runs on linux only")
}
