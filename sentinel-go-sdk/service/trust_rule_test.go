package service

import (
	"io/fs"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnixEntryTrusted(t *testing.T) {
	t.Parallel()

	const (
		root = 0
		user = 1000
	)

	cases := []struct {
		entry unixEntry
		euid  uint32
		kind  entryKind
		name  string
		want  bool
	}{
		{name: "root daemon: root file 0755", entry: unixEntry{mode: 0o755}, euid: root, want: true},
		{name: "root daemon: user-owned file", entry: unixEntry{mode: 0o755, uid: user}, euid: root},
		{name: "root daemon: user-owned homebrew folder", entry: unixEntry{mode: fs.ModeDir | 0o755, uid: 501}, euid: root, kind: entryParent},
		{name: "root daemon: group-writable root file", entry: unixEntry{mode: 0o775}, euid: root},
		{name: "root daemon: group-writable root folder", entry: unixEntry{mode: fs.ModeDir | 0o775}, euid: root, kind: entryParent},
		{name: "root daemon: world-writable folder above", entry: unixEntry{mode: fs.ModeDir | 0o777}, euid: root, kind: entryAncestor},
		{name: "root daemon: sticky tmp above", entry: unixEntry{mode: fs.ModeDir | fs.ModeSticky | 0o777}, euid: root, kind: entryAncestor, want: true},
		{name: "root daemon: sticky bit never saves a file", entry: unixEntry{mode: 0o777 | fs.ModeSticky}, euid: root},
		{name: "user: own file 0700", entry: unixEntry{mode: 0o700, uid: user}, euid: user, want: true},
		{name: "user: own group-writable file", entry: unixEntry{mode: 0o775, uid: user}, euid: user, want: true},
		{name: "user: own world-writable file", entry: unixEntry{mode: 0o757, uid: user}, euid: user},
		{name: "user: group-writable root file", entry: unixEntry{mode: 0o775}, euid: user},
		{name: "user: other user's file", entry: unixEntry{mode: 0o755, uid: 501}, euid: user},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, unixEntryTrusted(tc.entry, tc.euid, tc.kind), tc.name)
	}
}

func TestWindowsEntryTrusted(t *testing.T) {
	t.Parallel()

	const (
		self               = "S-1-5-21-1-2-3-1001"
		users              = "S-1-5-32-545"
		authenticatedUsers = "S-1-5-11"
		creatorOwner       = "S-1-3-0"
		readExecute        = uint32(0x0012_00a9)
	)

	trusted := []string{sidLocalSystem, sidAdministrators, sidTrustedInstaller, self}
	programFiles := []windowsACE{
		{mask: 0x001f_01ff, sid: sidTrustedInstaller},
		{mask: 0x001f_01ff, sid: sidLocalSystem},
		{mask: 0x001f_01ff, sid: sidAdministrators},
		{mask: readExecute, sid: users},
		{inheritOnly: true, mask: winGenericAll, sid: creatorOwner},
	}

	cases := []struct {
		dacl    []windowsACE
		kind    entryKind
		name    string
		owner   string
		present bool
		want    bool
	}{
		{name: "program files tool", owner: sidTrustedInstaller, dacl: programFiles, present: true, want: true},
		{name: "program files folder", owner: sidTrustedInstaller, dacl: programFiles, kind: entryParent, present: true, want: true},
		{name: "own file", owner: self, dacl: []windowsACE{{mask: 0x001f_01ff, sid: self}}, present: true, want: true},
		{name: "owned by a user", owner: "S-1-5-21-1-2-3-1002", dacl: programFiles, present: true},
		{name: "no access list", owner: sidAdministrators, present: false},
		{
			name: "users may modify the tool", owner: sidAdministrators, present: true,
			dacl: append([]windowsACE{{mask: 0x0013_01bf, sid: users}}, programFiles...),
		},
		{
			name: "users may add files next to the tool", owner: sidAdministrators, kind: entryParent, present: true,
			dacl: append([]windowsACE{{mask: readExecute | winFileWriteData, sid: users}}, programFiles...),
		},
		{
			name: "users may create subfolders on the drive root", owner: sidAdministrators, kind: entryAncestor, present: true,
			dacl: append([]windowsACE{{mask: readExecute | winFileAppendData, sid: authenticatedUsers}}, programFiles...),
			want: true,
		},
		{
			name: "users may delete children above", owner: sidAdministrators, kind: entryAncestor, present: true,
			dacl: append([]windowsACE{{mask: readExecute | winFileDeleteChild, sid: users}}, programFiles...),
		},
		{
			name: "users may rewrite the access list above", owner: sidAdministrators, kind: entryAncestor, present: true,
			dacl: append([]windowsACE{{mask: winWriteDAC, sid: users}}, programFiles...),
		},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, windowsEntryTrusted(tc.owner, tc.dacl, tc.present, trusted, tc.kind), tc.name)
	}
}
