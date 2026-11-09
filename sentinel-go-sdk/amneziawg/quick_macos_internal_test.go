package amneziawg

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// The lines of the darwin awg-quick (amneziawg-tools src/wg-quick/darwin.bash) this fix touches, in its shapes.
const quickExcerpt = `#!/usr/bin/env bash
get_real_interface() {
	wg show interfaces >/dev/null
	[[ -f "/var/run/amneziawg/$INTERFACE.name" ]] || return 1
	diff=$(( $(stat -f %m "/var/run/amneziawg/$interface.sock" 2>/dev/null || echo 200) - $(stat -f %m "/var/run/wireguard/$INTERFACE.name" 2>/dev/null || echo 100) ))
}
add_if() {
	cmd "${WG_QUICK_USERSPACE_IMPLEMENTATION:-amneziawg-go}" utun
}
del_if() {
	cmd rm -f "/var/run/wireguard/$INTERFACE.name"
}
	done < <(wg show "$REAL_INTERFACE" endpoints)
	cmd wg setconf "$REAL_INTERFACE" <(echo "$WG_CONFIG")
	current_config="$(cmd wg showconf "$REAL_INTERFACE")"
	if ! get_real_interface || [[ " $(wg show interfaces) " != *" $REAL_INTERFACE "* ]]; then
echo "see wg-quick(8)"
`

func TestPatchQuickDarwin(t *testing.T) {
	t.Parallel()

	out, err := patchQuickDarwin([]byte(quickExcerpt), "/opt/homebrew/bin/awg", "/opt/homebrew/bin/amneziawg-go")
	require.NoError(t, err)

	text := string(out)
	require.NotContains(t, text, "/var/run/wireguard", "every path is amneziawg-go's")
	require.Equal(t, 0, len(wgCall.FindAllString(text, -1)), "no call to plain wg is left:\n%s", text)
	require.Equal(t, 5, strings.Count(text, "/opt/homebrew/bin/awg "), "each wg call now runs awg")
	require.Contains(t, text, "${WG_QUICK_USERSPACE_IMPLEMENTATION:-/opt/homebrew/bin/amneziawg-go}")
	require.Contains(t, text, "see wg-quick(8)", "wg-quick in text is not a call")
	require.Contains(t, text, `stat -f %m "/var/run/amneziawg/$INTERFACE.name"`,
		"get_real_interface compares the socket with the name file amneziawg-go wrote")
}

func TestPatchQuickDarwin_Refuses(t *testing.T) {
	t.Parallel()

	_, err := patchQuickDarwin([]byte("#!/bin/sh\nwg show\n"), "/usr/local/bin/awg", "/usr/local/bin/amneziawg-go")
	require.ErrorIs(t, err, errQuickUnrecognised, "a script that is not the darwin awg-quick")

	_, err = patchQuickDarwin([]byte(quickExcerpt), "/Users/a b/awg", "/usr/local/bin/amneziawg-go")
	require.ErrorIs(t, err, errUnsafeToolPath)
}

func fakeTools(t *testing.T, bashMajor int) darwinToolset {
	t.Helper()

	return darwinToolset{
		bashVersion: func(path string) (int, error) {
			if path == "/bin/bash" {
				return 3, nil
			}

			if bashMajor == 0 {
				return 0, errors.New("not installed")
			}

			return bashMajor, nil
		},
		bashes:   []string{"/opt/homebrew/bin/bash", "/bin/bash"},
		lookPath: func(name string) (string, error) { return "/opt/homebrew/bin/" + name, nil },
		readFile: func(string) ([]byte, error) { return []byte(quickExcerpt), nil },
	}
}

func TestPrepareDarwinQuick(t *testing.T) {
	t.Parallel()

	p := service.Placement{Interface: "dvpn-c0", ToolDir: t.TempDir()}

	bash, script, err := prepareDarwinQuick(fakeTools(t, 5), p)
	require.NoError(t, err)
	require.Equal(t, "/opt/homebrew/bin/bash", bash)
	require.Equal(t, filepath.Join(p.ToolDir, "dvpn-c0.awg-quick.bash"), script)

	raw, err := os.ReadFile(script) //#nosec G304
	require.NoError(t, err)
	require.Contains(t, string(raw), "/opt/homebrew/bin/awg setconf")

	owned, err := service.OwnsFile(script)
	require.NoError(t, err)
	require.True(t, owned, "the copy is removed with the session's other files")
}

func TestPrepareDarwinQuick_NeedsBash4(t *testing.T) {
	t.Parallel()

	_, _, err := prepareDarwinQuick(fakeTools(t, 0), service.Placement{Interface: "dvpn-c0", ToolDir: t.TempDir()})
	require.ErrorIs(t, err, service.ErrBinaryNotFound, "macOS's bash 3.2 can't run awg-quick")
}

func TestDarwinQuickChecks(t *testing.T) {
	t.Parallel()

	require.Empty(t, darwinQuickChecks(fakeTools(t, 5)), "nothing to warn about when the fixed copy can be made")

	checks := darwinQuickChecks(fakeTools(t, 0))
	require.Len(t, checks, 1)
	require.Equal(t, "service_platform", checks[0].Name)
	require.Equal(t, service.SeverityWarning, checks[0].Severity)
	require.Contains(t, checks[0].Detail, "bash 4")
}
