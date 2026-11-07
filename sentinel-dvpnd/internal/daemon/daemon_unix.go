//go:build unix

package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/activation"
)

const (
	socketMode         = 0o660
	socketProbeTimeout = time.Second
)

func Listen(ctx context.Context, endpoint, group string) (net.Listener, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("starting daemon listener: %w", err)
	}

	listeners, err := activatedListeners()
	if err == nil && len(listeners) == 1 {
		return listeners[0], nil
	}

	dialer := net.Dialer{}

	probeCtx, cancel := context.WithTimeout(ctx, socketProbeTimeout)
	defer cancel()

	if conn, dialErr := dialer.DialContext(probeCtx, "unix", endpoint); dialErr == nil {
		_ = conn.Close()

		return nil, errEndpointInUse
	} else if !errors.Is(dialErr, os.ErrNotExist) && !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return nil, fmt.Errorf("probing daemon socket: %w", dialErr)
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("starting daemon listener: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(endpoint), 0o755); err != nil {
		return nil, fmt.Errorf("creating the socket folder: %w", err)
	}

	if err := os.Remove(endpoint); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("removing stale socket: %w", err)
	}

	var listenConfig net.ListenConfig

	ln, err := listenConfig.Listen(ctx, "unix", endpoint)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", endpoint, err)
	}

	if err := os.Chmod(endpoint, socketMode); err != nil {
		_ = ln.Close()

		return nil, fmt.Errorf("setting socket mode: %w", err)
	}

	if group != "" {
		g, err := user.LookupGroup(group)
		if err != nil {
			_ = ln.Close()

			return nil, fmt.Errorf("looking up group %s: %w", group, err)
		}

		gid, _ := strconv.Atoi(g.Gid)
		if err := os.Chown(endpoint, -1, gid); err != nil {
			_ = ln.Close()

			return nil, fmt.Errorf("setting socket group: %w", err)
		}
	}

	return ln, nil
}

func activatedListeners() ([]net.Listener, error) {
	if runtime.GOOS != "linux" {
		return nil, nil
	}

	listeners, err := activation.Listeners()
	if err != nil {
		return nil, fmt.Errorf("activating daemon listener: %w", err)
	}

	return listeners, nil
}
