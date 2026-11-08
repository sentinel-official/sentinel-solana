package main_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	main "github.com/sentinel-official/sentinel-dvpnd/cmd/sentinel-dvpnd"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

func TestBackgroundDisposition(t *testing.T) {
	t.Parallel()

	dump, err := os.ReadFile("testdata/sfltool-dumpbtm.txt")
	require.NoError(t, err)

	cases := []struct {
		enabled bool
		found   bool
		label   string
		name    string
	}{
		{enabled: true, found: true, label: "co.sentinel.sentinel-dvpnd.c0", name: "enabled"},
		{found: true, label: "co.sentinel.sentinel-dvpnd.c1", name: "disabled"},
		{label: "co.sentinel.sentinel-dvpnd.c9", name: "missing"},
		{label: "co.sentinel.sentinel-dvpnd.c", name: "prefix of another"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enabled, found := main.BackgroundDisposition(dump, tc.label)
			require.Equal(t, tc.enabled, enabled)
			require.Equal(t, tc.found, found)
		})
	}
}

func TestCheckBackground(t *testing.T) {
	t.Parallel()

	dump, err := os.ReadFile("testdata/sfltool-dumpbtm.txt")
	require.NoError(t, err)

	warning := []service.Severity{service.SeverityWarning}

	cases := []struct {
		name   string
		plist  bool
		runErr error
		tag    instance.Tag
		want   []service.Severity
	}{
		{name: "not installed", tag: "c0"},
		{name: "sfltool missing", plist: true, runErr: exec.ErrNotFound, tag: "c0"},
		{name: "run error", plist: true, runErr: errors.New("not root"), tag: "c0", want: warning},
		{name: "not listed", plist: true, tag: "c9", want: warning},
		{name: "disabled", plist: true, tag: "c1", want: []service.Severity{service.SeverityError}},
		{name: "enabled", plist: true, tag: "c0", want: []service.Severity{service.SeverityOK}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			plist := filepath.Join(root, "Library", "LaunchDaemons", "co.sentinel.sentinel-dvpnd."+string(tc.tag)+".plist")
			require.NoError(t, os.MkdirAll(filepath.Dir(plist), 0o750))

			if tc.plist {
				require.NoError(t, os.WriteFile(plist, nil, 0o600))
			}

			fakeExec := func(_ context.Context, c service.Command) ([]byte, error) {
				require.Equal(t, service.Command{Args: []string{"dumpbtm"}, Path: "sfltool"}, c)

				return dump, tc.runErr
			}

			var got []service.Severity

			checks := main.CheckBackground(t.Context(), main.NewServiceHostForTest(fakeExec, nil, "darwin", nil, root), tc.tag)
			for _, check := range checks {
				require.Equal(t, "background", check.Name)

				got = append(got, check.Severity)
			}

			require.Equal(t, tc.want, got)
		})
	}
}

func TestLaunchdManager_Install(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("windows reports file permissions differently")
	}

	m, recorder, root := newLaunchdFixture(t, false)
	plist := filepath.Join(root, "Library", "LaunchDaemons", "co.sentinel.sentinel-dvpnd.c0.plist")
	logDir := filepath.Join(root, "Library", "Logs", "sentinel-dvpn")

	for range 2 {
		require.NoError(t, m.Install(t.Context(), &config.Loaded{Tag: "c0"}))

		data, err := os.ReadFile(plist) //#nosec G304
		require.NoError(t, err)
		require.Equal(t, main.RenderLaunchdPlist("/Applications/My R&D/sentinel-dvpnd", "c0"), string(data))

		info, err := os.Stat(logDir)
		require.NoError(t, err)
		require.Equal(t, os.ModeDir|0o755, info.Mode())
	}

	require.Empty(t, recorder.commands)
}

func TestLaunchdManager_Install_LogDirSymlink(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("windows reports file permissions differently")
	}

	m, _, root := newLaunchdFixture(t, false)
	target := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.Mkdir(target, 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "Library", "Logs"), 0o750))
	require.NoError(t, os.Symlink(target, filepath.Join(root, "Library", "Logs", "sentinel-dvpn")))

	require.ErrorIs(t, m.Install(t.Context(), &config.Loaded{Tag: "c0"}), main.ErrInvalidDir)

	info, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, os.ModeDir|0o700, info.Mode())
}

func TestLaunchdManager_Restart(t *testing.T) {
	t.Parallel()

	const (
		label = "system/co.sentinel.sentinel-dvpnd.c0"
		probe = "launchctl print " + label
	)

	cases := []struct {
		loaded bool
		name   string
	}{
		{loaded: true, name: "loaded"},
		{name: "not loaded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, recorder, root := newLaunchdFixture(t, tc.loaded)
			plist := filepath.Join(root, "Library", "LaunchDaemons", "co.sentinel.sentinel-dvpnd.c0.plist")
			want := []string{probe, probe, "launchctl bootstrap system " + plist, "launchctl kickstart " + label}

			if tc.loaded {
				want = []string{probe, "launchctl kickstart -k " + label}
			}

			require.NoError(t, m.Restart(t.Context(), "c0"))
			require.Equal(t, want, recorder.commands)
		})
	}
}

func TestLaunchdManager_Start(t *testing.T) {
	t.Parallel()

	const (
		kickstart = "launchctl kickstart system/co.sentinel.sentinel-dvpnd.c0"
		probe     = "launchctl print system/co.sentinel.sentinel-dvpnd.c0"
	)

	cases := []struct {
		loaded bool
		name   string
	}{
		{loaded: true, name: "loaded"},
		{name: "not loaded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, recorder, root := newLaunchdFixture(t, tc.loaded)
			plist := filepath.Join(root, "Library", "LaunchDaemons", "co.sentinel.sentinel-dvpnd.c0.plist")
			want := []string{probe, "launchctl bootstrap system " + plist, kickstart}

			if tc.loaded {
				want = []string{probe, kickstart}
			}

			require.NoError(t, m.Start(t.Context(), "c0"))
			require.Equal(t, want, recorder.commands)
		})
	}
}

func TestLaunchdManager_Status(t *testing.T) {
	t.Parallel()

	for loaded, want := range map[bool]string{false: "not loaded", true: "running"} {
		m, _, _ := newLaunchdFixture(t, loaded)

		state, err := m.Status(t.Context(), "c0")
		require.NoError(t, err)
		require.Equal(t, want, state)
	}
}

func TestLaunchdManager_Uninstall(t *testing.T) {
	t.Parallel()

	const bootout = "launchctl bootout system/co.sentinel.sentinel-dvpnd.c0"

	cases := []struct {
		loaded bool
		name   string
		want   []string
	}{
		{loaded: true, name: "loaded", want: []string{"launchctl print system/co.sentinel.sentinel-dvpnd.c0", bootout}},
		{name: "not loaded", want: []string{"launchctl print system/co.sentinel.sentinel-dvpnd.c0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, recorder, root := newLaunchdFixture(t, tc.loaded)
			plist := filepath.Join(root, "Library", "LaunchDaemons", "co.sentinel.sentinel-dvpnd.c0.plist")
			require.NoError(t, os.WriteFile(plist, nil, 0o600))

			installed, err := m.Installed(t.Context(), "c0")
			require.NoError(t, err)
			require.True(t, installed)
			require.NoError(t, m.Uninstall(t.Context(), "c0"))
			require.Equal(t, tc.want, recorder.commands)
			require.NoFileExists(t, plist)
			require.NoError(t, m.Uninstall(t.Context(), "c0"))
		})
	}
}

func TestRenderLaunchdPlist(t *testing.T) {
	t.Parallel()

	require.Equal(t, `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>co.sentinel.sentinel-dvpnd.c0</string>
	<key>ProgramArguments</key>
	<array>
		<string>/usr/local/bin/sentinel-dvpnd</string>
		<string>run</string>
		<string>--instance</string>
		<string>c0</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>StandardOutPath</key>
	<string>/Library/Logs/sentinel-dvpn/c0.log</string>
	<key>StandardErrorPath</key>
	<string>/Library/Logs/sentinel-dvpn/c0.log</string>
	<key>EnvironmentVariables</key>
	<dict>
		<key>PATH</key>
		<string>/usr/bin:/bin:/usr/sbin:/sbin</string>
	</dict>
</dict>
</plist>
`, main.RenderLaunchdPlist("/usr/local/bin/sentinel-dvpnd", "c0"))
}

func TestRenderLaunchdPlist_EscapesBinary(t *testing.T) {
	t.Parallel()

	plist := main.RenderLaunchdPlist("/opt/R&D/sentinel-dvpnd", "c0")

	require.Contains(t, plist, "<string>/opt/R&amp;D/sentinel-dvpnd</string>")
	require.NotContains(t, plist, "R&D")
}

func newLaunchdFixture(t *testing.T, loaded bool) (main.ServiceManager, *recordingExec, string) {
	t.Helper()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "Library", "LaunchDaemons"), 0o750))

	recorder := &recordingExec{}
	fakeExec := func(ctx context.Context, c service.Command) ([]byte, error) {
		_, _ = recorder.run(ctx, c)

		switch c.Args[0] {
		case "bootout":
			loaded = false
		case "bootstrap":
			loaded = true
		case "print":
			if !loaded {
				return nil, errors.New("could not find service")
			}
		}

		return []byte("system/co.sentinel.sentinel-dvpnd.c0 = {\n\tactive count = 1\n\tstate = running\n}\n"), nil
	}
	host := main.NewServiceHostForTest(
		fakeExec,
		func() (string, error) { return "/Applications/My R&D/sentinel-dvpnd", nil },
		"darwin",
		nil,
		root,
	)

	m, err := main.NewServiceManager(host)
	require.NoError(t, err)

	return m, recorder, root
}
