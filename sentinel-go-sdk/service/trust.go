package service

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// ─── Trusted executables ───
//
// The daemon runs as root (SYSTEM on Windows) and starts protocol tools by name. A tool runs only
// when no other account can replace it: the file and every folder above it must belong to a
// trusted account (root, SYSTEM, Administrators or TrustedInstaller, or the account this process
// runs as) and must not be writable by anyone else.

// ErrUntrustedBinary reports a tool that an account other than a trusted one could replace.
var ErrUntrustedBinary = errors.New("binary is replaceable by an untrusted account")

type entryKind uint8

const (
	// entryFile is the executable itself.
	entryFile entryKind = iota
	// entryParent is the folder that holds the executable.
	entryParent
	// entryAncestor is any folder above entryParent.
	entryAncestor
)

// unixEntry is the part of a file's metadata the unix rule reads.
type unixEntry struct {
	mode fs.FileMode
	uid  uint32
}

// unixEntryTrusted applies the unix rule to one entry. The owner must be root or euid, and no group
// or other write bit may be set. An unprivileged process may still use its own entries that its
// group can write (umask 002 with per-user groups); a root process never does. A sticky folder
// (such as /tmp) is allowed above the executable, because other accounts cannot rename or remove
// entries they do not own inside it.
func unixEntryTrusted(entry unixEntry, euid uint32, kind entryKind) bool {
	if entry.uid != 0 && entry.uid != euid {
		return false
	}

	forbidden := fs.FileMode(0o022)
	if euid != 0 && entry.uid == euid {
		forbidden = 0o002
	}

	if entry.mode.Perm()&forbidden == 0 {
		return true
	}

	return kind != entryFile && entry.mode&fs.ModeSticky != 0
}

// Windows access rights that let an account replace a file or what a folder holds.
const (
	winFileWriteData   uint32 = 0x0000_0002 // FILE_WRITE_DATA, FILE_ADD_FILE on a folder
	winFileAppendData  uint32 = 0x0000_0004 // FILE_APPEND_DATA, FILE_ADD_SUBDIRECTORY on a folder
	winFileDeleteChild uint32 = 0x0000_0040
	winDelete          uint32 = 0x0001_0000
	winWriteDAC        uint32 = 0x0004_0000
	winWriteOwner      uint32 = 0x0008_0000
	winGenericAll      uint32 = 0x1000_0000
	winGenericWrite    uint32 = 0x4000_0000

	winReplaceRights = winDelete | winWriteDAC | winWriteOwner | winGenericAll | winGenericWrite
)

// Well-known SIDs a Windows service may trust.
const (
	sidAdministrators   = "S-1-5-32-544"
	sidLocalSystem      = "S-1-5-18"
	sidTrustedInstaller = "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464"
)

// windowsACE is one access-allowed entry of a DACL.
type windowsACE struct {
	inheritOnly bool
	mask        uint32
	sid         string
}

// windowsDeniedRights is what an untrusted account must not hold on an entry. On the executable
// that is any write; on its folder, adding a file (a planted DLL is loaded from there) or removing
// one; on folders above, removing or renaming what they hold. Creating new subfolders higher up
// (granted on C:\ by default) cannot replace an existing tool and stays allowed.
func windowsDeniedRights(kind entryKind) uint32 {
	switch kind {
	case entryFile:
		return winReplaceRights | winFileWriteData | winFileAppendData
	case entryParent:
		return winReplaceRights | winFileWriteData | winFileDeleteChild
	default:
		return winReplaceRights | winFileDeleteChild
	}
}

// windowsEntryTrusted applies the Windows rule to one entry: a trusted owner and no allow entry
// that grants an untrusted SID a denied right. A missing DACL grants everyone full access.
func windowsEntryTrusted(owner string, dacl []windowsACE, daclPresent bool, trusted []string, kind entryKind) bool {
	if !slices.Contains(trusted, owner) || !daclPresent {
		return false
	}

	denied := windowsDeniedRights(kind)

	for _, ace := range dacl {
		if ace.inheritOnly || ace.mask&denied == 0 {
			continue
		}

		if !slices.Contains(trusted, ace.sid) {
			return false
		}
	}

	return true
}

// trustedExecutable checks an absolute path to an executable, both as written and with symlinks
// resolved, from the file up to the root.
func trustedExecutable(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%w: %s is not an absolute path", ErrUntrustedBinary, path)
	}

	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolving %s: %w", path, err)
	}

	for _, candidate := range slices.Compact([]string{path, resolved}) {
		if err := checkEntry(candidate, entryFile); err != nil {
			return err
		}

		if err := trustedChain(filepath.Dir(candidate)); err != nil {
			return err
		}
	}

	return nil
}

// trustedChain checks a folder that holds executables, and every folder above it.
func trustedChain(dir string) error {
	kind := entryParent

	for {
		if err := checkEntry(dir, kind); err != nil {
			return err
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}

		dir, kind = parent, entryAncestor
	}
}

// trustedDir reports whether an absolute folder (and every folder above it) passes the rule.
func trustedDir(dir string) bool {
	if !filepath.IsAbs(dir) {
		return false
	}

	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return false
	}

	for _, candidate := range slices.Compact([]string{dir, resolved}) {
		if trustedChain(candidate) != nil {
			return false
		}
	}

	return true
}

// knownDirs is knownBinaryDirs; tests replace it to point the fallback at their own folders.
var knownDirs = knownBinaryDirs

// uniqueAbsDirs joins folder lists in order, dropping empty, relative and repeated entries.
func uniqueAbsDirs(lists ...[]string) []string {
	var dirs []string

	for _, dir := range slices.Concat(lists...) {
		if dir == "" || !filepath.IsAbs(dir) || slices.Contains(dirs, dir) {
			continue
		}

		dirs = append(dirs, dir)
	}

	return dirs
}

// searchDirs lists where a bare tool name is looked up, in order: PATH, the platform's usual
// install folders, then the folders protocol drivers register. Relative PATH entries are skipped.
// Every candidate found in any of them must still pass the rule (see LookPath).
func searchDirs(goos string) []string {
	return uniqueAbsDirs(filepath.SplitList(os.Getenv("PATH")), knownDirs(goos), registeredDirs())
}

// trustedPathEnv is the PATH a started tool gets: only folders that pass the rule, so a script
// such as wg-quick cannot reach a replaceable helper by name. The tool folders (registered by
// drivers, then the platform's usual install folders) come before the process PATH, so a script's
// helpers come from where its tools live: on macOS, `#!/usr/bin/env bash` in wg-quick must find a
// bash 4+ installed with the tools rather than the system bash 3.2 in /bin.
func trustedPathEnv(goos string) string {
	dirs := uniqueAbsDirs(registeredDirs(), knownDirs(goos), filepath.SplitList(os.Getenv("PATH")))

	return strings.Join(slices.DeleteFunc(dirs, func(dir string) bool { return !trustedDir(dir) }),
		string(os.PathListSeparator))
}
