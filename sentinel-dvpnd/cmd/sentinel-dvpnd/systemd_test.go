package main_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	main "github.com/sentinel-official/sentinel-dvpnd/cmd/sentinel-dvpnd"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/amneziawg"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/openvpn"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/wireguard"
)

type recordingKernel struct {
	asked     []string
	supported map[string]bool
}

func (k *recordingKernel) probe(_ context.Context, kind string) (bool, error) {
	k.asked = append(k.asked, kind)

	return k.supported[kind], nil
}

func TestPrepareToolDirs(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("windows keeps tool files in the state folder")
	}

	loaded := &config.Loaded{
		Dir:      t.TempDir(),
		Services: map[service.Name]any{openvpn.Name: nil, wireguard.Name: nil},
		Tag:      "c0",
	}
	dirs := main.InstanceToolDirs(loaded)
	require.Equal(t, []string{"/etc/openvpn/client/sentinel-dvpn", "/etc/wireguard/sentinel-dvpn"}, dirs)

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc", "wireguard"), 0o700))

	require.NoError(t, main.PrepareToolDirs(root, dirs))
	require.NoError(t, main.PrepareToolDirs(root, dirs))
	requireMode(t, filepath.Join(root, "etc/openvpn/client/sentinel-dvpn"), 0o700)
	requireMode(t, filepath.Join(root, "etc/openvpn/client"), 0o755)
	requireMode(t, filepath.Join(root, "etc/openvpn"), 0o755)
	requireMode(t, filepath.Join(root, "etc/wireguard"), 0o700)
}

func TestRenderSystemdService(t *testing.T) {
	t.Parallel()

	unit := main.RenderSystemdService("/usr/local/bin/sentinel-dvpnd")

	require.Equal(t, `[Unit]
Description=Sentinel dVPN daemon (%i)
Requires=sentinel-dvpnd@%i.socket
After=network-online.target sentinel-dvpnd@%i.socket
Wants=network-online.target

[Service]
Type=notify
ExecStart=/usr/local/bin/sentinel-dvpnd run --instance %i
Restart=always
RestartSec=5
TimeoutStopSec=180
KillMode=mixed
AmbientCapabilities=CAP_NET_ADMIN CAP_NET_BIND_SERVICE
# NoNewPrivileges stays off: wg-quick switches wg, ip, nft and sysctl into their AppArmor profiles,
# which the kernel refuses under no_new_privs. systemd turns NoNewPrivileges on by itself when a unit
# without CAP_SYS_ADMIN uses seccomp options (SystemCallFilter=, RestrictAddressFamilies=, ...), so
# adding User= together with such options brings that breakage back.
ProtectSystem=strict
PrivateTmp=yes
ReadWritePaths=/var/lib/sentinel-dvpn/%i
ReadWritePaths=-/etc/resolv.conf
ReadWritePaths=-/etc/amnezia/amneziawg/sentinel-dvpn
ReadWritePaths=-/etc/hysteria/sentinel-dvpn
ReadWritePaths=-/etc/openvpn/client/sentinel-dvpn
ReadWritePaths=-/etc/openvpn/server/sentinel-dvpn
ReadWritePaths=-/etc/wireguard/sentinel-dvpn
ReadWritePaths=-/usr/local/etc/v2ray/sentinel-dvpn
ReadWritePaths=-/usr/local/etc/xray/sentinel-dvpn
EnvironmentFile=-/etc/sentinel-dvpn/%i.env

[Install]
WantedBy=multi-user.target
`, unit)
	require.NotContains(t, unit, "StateDirectory")
}

func TestRenderSystemdSocket(t *testing.T) {
	t.Parallel()

	require.Equal(t, `[Unit]
Description=Sentinel dVPN daemon control socket (%i)

[Socket]
ListenStream=/run/sentinel-dvpn/%i/daemon.sock
Symlinks=/var/lib/sentinel-dvpn/%i/daemon.sock
SocketMode=0660
SocketGroup=sentinel-dvpn
RemoveOnStop=yes

[Install]
WantedBy=sockets.target
`, main.RenderSystemdSocket())
}

func TestSystemdManager_Install(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("windows reports file permissions differently")
	}

	const binary = "/usr/local/bin/sentinel-dvpnd"

	m, recorder, dir, kernel := newSystemdFixture(t, binary)
	dropIn := filepath.Join(dir, "sentinel-dvpnd@n0.service.d", "userspace.conf")

	masked := filepath.Join(t.TempDir(), "masked")
	require.NoError(t, os.WriteFile(masked, []byte("keep"), 0o600))
	require.NoError(t, os.Symlink(masked, filepath.Join(dir, "sentinel-dvpnd@.service")))

	loaded := &config.Loaded{Services: map[service.Name]any{wireguard.Name: nil}, Tag: "n0"}
	want := []string{
		"systemctl daemon-reload",
		"systemctl enable sentinel-dvpnd@n0.socket sentinel-dvpnd@n0.service",
	}

	for range 2 {
		recorder.commands = nil

		require.NoError(t, m.Install(t.Context(), loaded))
		require.Equal(t, want, recorder.commands)
		require.FileExists(t, dropIn)

		for name, content := range map[string]string{
			"sentinel-dvpnd@.service": main.RenderSystemdService(binary),
			"sentinel-dvpnd@.socket":  main.RenderSystemdSocket(),
		} {
			path := filepath.Join(dir, name)

			data, err := os.ReadFile(path) //#nosec G304
			require.NoError(t, err)
			require.Equal(t, content, string(data))

			info, err := os.Lstat(path)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o644), info.Mode())
		}
	}

	kernel.supported = map[string]bool{"wireguard": true}

	require.NoError(t, m.Install(t.Context(), loaded))
	require.NoFileExists(t, dropIn)

	requireMode(t, filepath.Join(dir, "..", "..", "..", "etc", "wireguard", "sentinel-dvpn"), 0o700)

	kept, err := os.ReadFile(masked) //#nosec G304
	require.NoError(t, err)
	require.Equal(t, "keep", string(kept))

	info, err := os.Stat(masked)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode())
}

func TestSystemdManager_Install_InvalidBinaryPath(t *testing.T) {
	t.Parallel()

	m, recorder, dir, _ := newSystemdFixture(t, "/opt/my sentinel-dvpnd/bin")

	require.ErrorIs(t, m.Install(t.Context(), &config.Loaded{Tag: "n0"}), main.ErrInvalidBinaryPath)
	require.Empty(t, recorder.commands)
	require.NoFileExists(t, filepath.Join(dir, "sentinel-dvpnd@.service"))
}

func TestSystemdManager_Install_InvalidDropInDir(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("windows reports file permissions differently")
	}

	m, _, dir, kernel := newSystemdFixture(t, "/usr/local/bin/sentinel-dvpnd")
	loaded := &config.Loaded{Services: map[service.Name]any{wireguard.Name: nil}, Tag: "n0"}

	target := t.TempDir()
	conf := filepath.Join(target, "userspace.conf")
	require.NoError(t, os.WriteFile(conf, []byte("keep"), 0o600))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "sentinel-dvpnd@n0.service.d")))

	for _, supported := range []bool{false, true} {
		kernel.supported = map[string]bool{"wireguard": supported}

		require.ErrorIs(t, m.Install(t.Context(), loaded), main.ErrInvalidDir)
	}

	data, err := os.ReadFile(conf) //#nosec G304
	require.NoError(t, err)
	require.Equal(t, "keep", string(data))
	requireMode(t, target, 0o700)
}

func TestSystemdManager_Installed(t *testing.T) {
	t.Parallel()

	m, _, dir, _ := newSystemdFixture(t, "/usr/local/bin/sentinel-dvpnd")
	wants := filepath.Join(dir, "multi-user.target.wants")
	require.NoError(t, os.MkdirAll(wants, 0o750))
	require.NoError(t, os.Symlink("../sentinel-dvpnd@.service", filepath.Join(wants, "sentinel-dvpnd@n0.service")))

	for tag, want := range map[instance.Tag]bool{"n0": true, "n1": false} {
		got, err := m.Installed(t.Context(), tag)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestSystemdManager_Prune(t *testing.T) {
	t.Parallel()

	m, recorder, dir, _ := newSystemdFixture(t, "/usr/local/bin/sentinel-dvpnd")
	service := filepath.Join(dir, "sentinel-dvpnd@.service")
	require.NoError(t, os.WriteFile(service, nil, 0o600))

	require.NoError(t, m.Prune(t.Context()))
	require.NoFileExists(t, service)
	require.NoFileExists(t, filepath.Join(dir, "sentinel-dvpnd@.socket"))
	require.Equal(t, []string{"systemctl daemon-reload"}, recorder.commands)
}

func TestSystemdManager_Start(t *testing.T) {
	t.Parallel()

	m, recorder, _, _ := newSystemdFixture(t, "/usr/local/bin/sentinel-dvpnd")

	require.NoError(t, m.Start(t.Context(), "n0"))
	require.Equal(t, []string{"systemctl start sentinel-dvpnd@n0.service"}, recorder.commands)
}

func TestSystemdManager_Status(t *testing.T) {
	t.Parallel()

	recorder := &recordingExec{}
	fakeExec := func(ctx context.Context, c service.Command) ([]byte, error) {
		_, _ = recorder.run(ctx, c)

		return []byte("active\n"), nil
	}

	m, err := main.NewServiceManager(main.NewServiceHostForTest(fakeExec, nil, "linux", nil, t.TempDir()))
	require.NoError(t, err)

	state, err := m.Status(t.Context(), "n0")
	require.NoError(t, err)
	require.Equal(t, "active", state)
	require.Equal(
		t,
		[]string{"systemctl show --property=ActiveState --value sentinel-dvpnd@n0.service"},
		recorder.commands,
	)
}

func TestSystemdManager_Stop(t *testing.T) {
	t.Parallel()

	m, recorder, _, _ := newSystemdFixture(t, "/usr/local/bin/sentinel-dvpnd")

	require.NoError(t, m.Stop(t.Context(), "n0"))
	require.Equal(t, []string{"systemctl stop sentinel-dvpnd@n0.socket sentinel-dvpnd@n0.service"}, recorder.commands)
}

func TestSystemdManager_Uninstall(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dir := filepath.Join(root, "etc", "systemd", "system")
	dropIn := filepath.Join(dir, "sentinel-dvpnd@n0.service.d", "userspace.conf")
	recorder := &recordingExec{}
	fakeExec := func(ctx context.Context, c service.Command) ([]byte, error) {
		if c.Args[0] == "daemon-reload" {
			require.NoFileExists(t, dropIn)
		}

		return recorder.run(ctx, c)
	}

	m, err := main.NewServiceManager(main.NewServiceHostForTest(fakeExec, nil, "linux", nil, root))
	require.NoError(t, err)

	require.NoError(t, m.Uninstall(t.Context(), "n0"))
	require.Empty(t, recorder.commands)

	require.NoError(t, os.MkdirAll(filepath.Dir(dropIn), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sentinel-dvpnd@.service"), nil, 0o600))
	require.NoError(t, os.WriteFile(dropIn, nil, 0o600))
	require.NoError(t, m.Uninstall(t.Context(), "n0"))
	require.Equal(t, []string{
		"systemctl disable --now sentinel-dvpnd@n0.service sentinel-dvpnd@n0.socket",
		"systemctl daemon-reload",
	}, recorder.commands)
}

func TestUserspaceNames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		asked    []string
		kernel   map[string]bool
		name     string
		services []service.Name
		want     []service.Name
	}{
		{
			asked:    []string{"wireguard"},
			kernel:   map[string]bool{"wireguard": true},
			name:     "node with kernel wireguard",
			services: []service.Name{wireguard.Name},
		},
		{
			asked:    []string{"amneziawg"},
			name:     "node without kernel amneziawg",
			services: []service.Name{amneziawg.Name},
			want:     []service.Name{amneziawg.Name},
		},
		{
			asked:    []string{"amneziawg", "wireguard"},
			kernel:   map[string]bool{"wireguard": true},
			name:     "client tests every enabled tunnel",
			services: []service.Name{amneziawg.Name, openvpn.Name, wireguard.Name},
			want:     []service.Name{amneziawg.Name},
		},
		{name: "provider"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			loaded := &config.Loaded{Services: map[service.Name]any{}}
			for _, name := range tc.services {
				loaded.Services[name] = nil
			}

			kernel := &recordingKernel{supported: tc.kernel}

			got, err := main.UserspaceNames(t.Context(), loaded, kernel.probe)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.asked, kernel.asked)
		})
	}
}

func TestWriteUserspaceDropIn(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosWindows {
		t.Skip("windows reports file permissions differently")
	}

	root := t.TempDir()
	dir := filepath.Join(root, "etc", "systemd", "system", "sentinel-dvpnd@n1.service.d")
	path := filepath.Join(dir, "userspace.conf")
	require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o750))

	names := []service.Name{wireguard.Name, amneziawg.Name}
	require.NoError(t, main.WriteUserspaceDropIn(root, "n1", names))

	const want = "[Service]\nRuntimeDirectory=amneziawg\nRuntimeDirectory=wireguard\nRuntimeDirectoryPreserve=yes\n"

	data, err := os.ReadFile(path) //#nosec G304
	require.NoError(t, err)
	require.Equal(t, want, string(data))
	requireMode(t, dir, 0o755)

	other := filepath.Join(dir, "other.conf")
	require.NoError(t, os.WriteFile(other, nil, 0o600))
	require.NoError(t, main.WriteUserspaceDropIn(root, "n1", nil))
	require.NoFileExists(t, path)
	require.FileExists(t, other)

	require.NoError(t, os.Remove(other))
	require.NoError(t, main.WriteUserspaceDropIn(root, "n1", nil))
	require.NoDirExists(t, dir)
	require.NoError(t, main.WriteUserspaceDropIn(root, "n1", nil))
}

func newSystemdFixture(t *testing.T, binary string) (main.ServiceManager, *recordingExec, string, *recordingKernel) {
	t.Helper()

	root := t.TempDir()
	dir := filepath.Join(root, "etc", "systemd", "system")
	require.NoError(t, os.MkdirAll(dir, 0o750))

	recorder := &recordingExec{}
	kernel := &recordingKernel{}
	executable := func() (string, error) { return binary, nil }

	m, err := main.NewServiceManager(main.NewServiceHostForTest(recorder.run, executable, "linux", kernel.probe, root))
	require.NoError(t, err)

	return m, recorder, dir, kernel
}

func requireMode(t *testing.T, path string, mode os.FileMode) {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, mode, info.Mode().Perm())
}
