//go:build !windows

package service

import (
	"fmt"
	"os"
	"syscall"
)

//nolint:gochecknoglobals // tests replace it to stand in for another account
var trustEUID = os.Geteuid

func checkEntry(path string, kind entryKind) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("checking %s: %w", path, err)
	}

	if kind == entryFile && !info.Mode().IsRegular() {
		return fmt.Errorf("%w: %s is not a regular file", ErrUntrustedBinary, path)
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%w: %s: no owner information", ErrUntrustedBinary, path)
	}

	entry := unixEntry{mode: info.Mode(), uid: stat.Uid}
	if !unixEntryTrusted(entry, uint32(trustEUID()), kind) { //nolint:gosec // uids fit in uint32
		return fmt.Errorf("%w: %s (owner uid %d, mode %s)", ErrUntrustedBinary, path, stat.Uid, info.Mode())
	}

	return nil
}
