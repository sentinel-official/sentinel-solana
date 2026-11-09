package client

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
)

// writeTunnelIface records the live tunnel interface name in the instance state dir. A CLI invoked
// while the daemon is down (e.g. after a hard kill) reads it to name the leftover interface the
// kill left behind. An empty dir or iface writes nothing; a write failure is logged, never fatal.
func writeTunnelIface(dir, iface string, log zerolog.Logger) {
	if dir == "" || iface == "" {
		return
	}

	path := filepath.Join(dir, instance.TunnelIfaceFileName)

	// Atomic: write a temp file in the same dir (so rename stays on one filesystem), then rename
	// over the marker. A reader never sees a half-written name, and a crash leaves at most a temp.
	tmp, err := os.CreateTemp(dir, instance.TunnelIfaceFileName+".*")
	if err != nil {
		log.Warn().Err(err).Str("dir", dir).Msg("recording tunnel interface marker")

		return
	}

	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once the rename below succeeds

	if _, err := tmp.WriteString(iface + "\n"); err != nil {
		_ = tmp.Close()
		log.Warn().Err(err).Str("path", tmpName).Msg("recording tunnel interface marker")

		return
	}

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		log.Warn().Err(err).Str("path", tmpName).Msg("recording tunnel interface marker")

		return
	}

	if err := tmp.Close(); err != nil {
		log.Warn().Err(err).Str("path", tmpName).Msg("recording tunnel interface marker")

		return
	}

	if err := os.Rename(tmpName, path); err != nil {
		log.Warn().Err(err).Str("path", path).Msg("recording tunnel interface marker")
	}
}

// readTunnelIface returns the recorded live tunnel interface name, or "" when none is recorded or
// it cannot be read. Recovery uses it to remove the exact interface a session left behind.
func readTunnelIface(dir string) string {
	if dir == "" {
		return ""
	}

	raw, err := os.ReadFile(filepath.Join(dir, instance.TunnelIfaceFileName))
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(raw))
}

// removeTunnelIface clears the marker on teardown or recovery. A missing marker is not an error.
func removeTunnelIface(dir string, log zerolog.Logger) {
	if dir == "" {
		return
	}

	path := filepath.Join(dir, instance.TunnelIfaceFileName)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Warn().Err(err).Str("path", path).Msg("removing tunnel interface marker")
	}
}
