package config_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/creasty/defaults"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/openvpn"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
	"github.com/sentinel-official/sentinel-go-sdk/v3/wireguard"
)

const (
	goosDarwin  = "darwin"
	goosLinux   = "linux"
	goosWindows = "windows"
)

func TestAccessGroup(t *testing.T) {
	t.Parallel()

	require.Equal(t, instance.GroupName, config.AccessGroup("c0", instance.Tag("c0").StateDir(runtime.GOOS)))
	require.Empty(t, config.AccessGroup("c0", t.TempDir()))
}

func TestDefaultFile(t *testing.T) {
	t.Parallel()

	var want config.File
	require.NoError(t, defaults.Set(&want))
	want.Log = cli.LogConfig{Format: cli.LogFormatJSON, Level: "info"}
	want.RPC = cli.DefaultRPCConfig(cli.NetworkLocalnet)
	want.Tx = tx.DefaultConfig()
	require.Equal(t, want, config.DefaultFile(cli.NetworkLocalnet))
}

func TestDetectCLIEnv(t *testing.T) {
	t.Parallel()

	got, err := config.DetectCLIEnv([]string{
		"SENTINEL_DVPN_RPC_URL=http://127.0.0.1:8899", "SENTINEL_DVPN_LEASE_RESERVE_HOURS=7",
		"SENTINEL_DVPN_PASSPHRASE=secret", "SENTINEL_DVPN_E2E_REQUIRE=1", "SENTINEL_DVPND_RPC_URL=x", "PATH=/usr/bin",
	})
	require.NoError(t, err)
	require.Equal(t, []config.EnvMismatch{
		{Actual: "SENTINEL_DVPN_LEASE_RESERVE_HOURS", Expected: "SENTINEL_DVPND_LEASE_RESERVE_HOURS"},
		{Actual: "SENTINEL_DVPN_PASSPHRASE", Expected: "SENTINEL_DVPND_PASSPHRASE"},
		{Actual: "SENTINEL_DVPN_RPC_URL", Expected: "SENTINEL_DVPND_RPC_URL"},
	}, got)
}

func TestDir(t *testing.T) {
	t.Parallel()

	dir, err := config.Dir("c0", "")
	require.NoError(t, err)
	require.Equal(t, instance.Tag("c0").StateDir(runtime.GOOS), dir)

	dir, err = config.Dir("c0", "rel")
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(dir))
}

func TestDNSBackends(t *testing.T) {
	t.Parallel()

	cases := map[string][]string{
		goosDarwin:  {config.DNSNetworksetup, config.DNSNone},
		goosLinux:   {config.DNSResolvectl, config.DNSResolvconf, config.DNSNone},
		goosWindows: {config.DNSNetsh, config.DNSNone},
	}
	for goos, want := range cases {
		require.Equal(t, want, config.DNSBackendsFor(goos), goos)
		require.Equal(t, want[0], config.DefaultDNSBackendFor(goos), goos)
	}

	require.Equal(t, config.DNSBackendsFor(runtime.GOOS), config.DNSBackends())
	require.Equal(t, config.DefaultDNSBackendFor(runtime.GOOS), config.DefaultDNSBackend())
}

func TestLoad_DefaultsMissingTxMaxTransactionBytes(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != goosLinux {
		t.Skip("the provider role is supported on linux only")
	}

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleProvider
	cfg := config.DefaultProvider()
	cfg.API.Hosts = []string{"203.0.113.1"}
	cfg.ID = strings.Repeat("a", 64)
	cfg.Owner = (solana.PublicKey{1}).String()
	require.NoError(t, config.Save(home, file, cfg))

	path := filepath.Join(home, config.FileName)
	raw, err := os.ReadFile(path) //#nosec G304
	require.NoError(t, err)
	require.Contains(t, string(raw), "max_transaction_bytes = 1232\n")
	raw = []byte(strings.Replace(string(raw), "max_transaction_bytes = 1232\n", "", 1))
	require.NoError(t, os.WriteFile(path, raw, 0o600)) //#nosec G703

	loaded, err := config.Load("p0", home, newFlags(t))
	require.NoError(t, err)
	require.Equal(t, 1232, loaded.File.Tx.MaxTransactionBytes)

	raw = []byte(strings.Replace(string(raw), "[tx]\n", "[tx]\nmax_transaction_bytes = 0\n", 1))
	require.NoError(t, os.WriteFile(path, raw, 0o600)) //#nosec G703
	_, err = config.Load("p0", home, newFlags(t))
	require.ErrorContains(t, err, "max_transaction_bytes gt=0")
}

func TestLoad_EnvOverridesAbsentKey(t *testing.T) {
	if runtime.GOOS != goosLinux {
		t.Skip("the provider role is supported on linux only")
	}

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleProvider
	cfg := config.DefaultProvider()
	cfg.API.Hosts = []string{"203.0.113.1"}
	cfg.ID = strings.Repeat("a", 64)
	cfg.Owner = (solana.PublicKey{1}).String()
	require.NoError(t, config.Save(home, file, cfg))

	path := filepath.Join(home, config.FileName)
	raw, err := os.ReadFile(path) //#nosec G304
	require.NoError(t, err)
	require.Contains(t, string(raw), "reserve_hours = 24")
	raw = []byte(strings.Replace(string(raw), "reserve_hours = 24\n", "", 1))
	require.NoError(t, os.WriteFile(path, raw, 0o600)) //#nosec G703
	t.Setenv("SENTINEL_DVPND_LEASE_RESERVE_HOURS", "7")

	loaded, err := config.Load("p0", home, newFlags(t))
	require.NoError(t, err)
	require.EqualValues(t, 7, loaded.Provider.Lease.ReserveHours)
}

func TestLoad_InstanceNotFound(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	for _, name := range []string{"n0", "p1", "notes"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, name), 0o700))
	}

	_, err := config.Load("c0", filepath.Join(root, "c0"), newFlags(t))
	require.ErrorIs(t, err, instance.ErrInstanceNotFound)
	require.EqualError(t, err, "instance c0 not found; available: n0, p1")
}

func TestLoad_MissingAPIHosts(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != goosLinux {
		t.Skip("the node role is supported on linux only")
	}

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleNode
	cfg := config.DefaultNode()
	cfg.API.Hosts = nil
	cfg.Country = "DE"
	cfg.ID = strings.Repeat("a", 64)
	cfg.Owner = (solana.PublicKey{1}).String()
	require.NoError(t, config.Save(home, file, cfg))
	require.NoError(t, config.SaveService(home, wireguard.Name, wireguard.DefaultServerConfig()))

	_, err := config.Load("n0", home, newFlags(t))
	require.ErrorContains(t, err, "hosts min=1")
}

func TestLoad_PaymasterFromEnv(t *testing.T) {
	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleClient
	cfg := config.DefaultClient()
	cfg.Owner = (solana.PublicKey{1}).String()
	require.NoError(t, config.Save(home, file, cfg))

	path := filepath.Join(home, config.FileName)
	raw, err := os.ReadFile(path) //#nosec G304
	require.NoError(t, err)

	head, _, found := strings.Cut(string(raw), "\n[tx.paymaster]")
	require.True(t, found)
	require.NoError(t, os.WriteFile(path, []byte(head+"\n"), 0o600)) //#nosec G703
	t.Setenv("SENTINEL_DVPND_TX_PAYMASTER_URL", "http://127.0.0.1:8080")
	t.Setenv("SENTINEL_DVPND_TX_PAYMASTER_MAX_FEE_BASE_UNITS", "5000")

	loaded, err := config.Load("c0", home, newFlags(t))
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:8080", loaded.File.Tx.Paymaster.URL)
	require.EqualValues(t, 5000, loaded.File.Tx.Paymaster.MaxFeeBaseUnits)
}

func TestLoad_PeerAllowPrivate(t *testing.T) {
	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleClient
	cfg := config.DefaultClient()
	cfg.Owner = (solana.PublicKey{1}).String()
	require.NoError(t, config.Save(home, file, cfg))

	path := filepath.Join(home, config.FileName)
	raw, err := os.ReadFile(path) //#nosec G304
	require.NoError(t, err)

	cases := []struct {
		env  string
		file string
		flag string
		name string
		want bool
	}{
		{
			name: "default",
			want: false,
		},
		{
			env:  "true",
			name: "env",
			want: true,
		},
		{
			file: "true",
			name: "file",
			want: true,
		},
		{
			flag: "true",
			name: "flag",
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SENTINEL_DVPND_PEER_ALLOW_PRIVATE", tc.env)

			body := raw
			if tc.file != "" {
				body = []byte(strings.Replace(string(raw), "allow_private = false", "allow_private = "+tc.file, 1))
			}

			require.NoError(t, os.WriteFile(path, body, 0o600)) //#nosec G703

			flags := newFlags(t)
			if tc.flag != "" {
				require.NoError(t, flags.Set("peer.allow-private", tc.flag))
			}

			loaded, err := config.Load("c0", home, flags)
			require.NoError(t, err)
			require.Equal(t, tc.want, loaded.File.Peer.AllowPrivate)
		})
	}
}

func TestLoad_RejectsHeartbeatAboveBound(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != goosLinux {
		t.Skip("the node role is supported on linux only")
	}

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleNode
	cfg := config.DefaultNode()
	cfg.API.Hosts = []string{"203.0.113.1"}
	cfg.Country = "DE"
	cfg.ID = strings.Repeat("a", 64)
	cfg.Owner = (solana.PublicKey{1}).String()
	cfg.Sessions.HeartbeatSeconds = 61
	require.NoError(t, config.Save(home, file, cfg))
	require.NoError(t, config.SaveService(home, wireguard.Name, wireguard.DefaultServerConfig()))

	_, err := config.Load("n0", home, newFlags(t))
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.ErrorContains(t, err, "heartbeat_seconds lte=60")
}

func TestLoad_RejectsInvalidServiceConfig(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != goosLinux {
		t.Skip("the node role is supported on linux only")
	}

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleNode
	cfg := config.DefaultNode()
	cfg.API.Hosts = []string{"203.0.113.1"}
	cfg.Country = "DE"
	cfg.ID = strings.Repeat("a", 64)
	cfg.Owner = (solana.PublicKey{1}).String()
	cfg.Service = wireguard.Name.String()
	serverCfg := wireguard.DefaultServerConfig()
	serverCfg.ListenPort = 80

	require.NoError(t, config.Save(home, file, cfg))
	require.NoError(t, config.SaveService(home, wireguard.Name, serverCfg))

	_, err := config.Load("n0", home, newFlags(t))
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.ErrorContains(t, err, filepath.Join("wireguard", config.FileName))
}

func TestLoad_RejectsNonASCIICountry(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != goosLinux {
		t.Skip("the node role is supported on linux only")
	}

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleNode
	cfg := config.DefaultNode()
	cfg.API.Hosts = []string{"203.0.113.1"}
	cfg.Country = "ÉÉ"
	cfg.ID = strings.Repeat("a", 64)
	cfg.Owner = (solana.PublicKey{1}).String()
	require.NoError(t, config.Save(home, file, cfg))
	require.NoError(t, config.SaveService(home, wireguard.Name, wireguard.DefaultServerConfig()))

	_, err := config.Load("n0", home, newFlags(t))
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.ErrorContains(t, err, "country")
}

func TestLoad_RejectsRoleMismatch(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleClient
	require.NoError(t, config.Save(home, file, nil))
	_, err := config.Load("n0", home, newFlags(t))
	require.ErrorIs(t, err, config.ErrRoleMismatch)
}

func TestLoad_RejectsUnsupportedRole(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == goosLinux {
		t.Skip("every role is supported on linux")
	}

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleNode
	require.NoError(t, config.Save(home, file, nil))

	_, err := config.Load("n0", home, newFlags(t))
	require.ErrorIs(t, err, config.ErrUnsupportedRole)
	require.EqualError(t, err, "role is supported on linux only: node")
}

func TestLoad_RelativeHome(t *testing.T) {
	parent := t.TempDir()
	home := filepath.Join(parent, "c0")
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleClient
	cfg := config.DefaultClient()
	cfg.Owner = (solana.PublicKey{1}).String()
	require.NoError(t, config.Save(home, file, cfg))
	t.Chdir(parent)

	loaded, err := config.Load("c0", "c0", newFlags(t))
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(loaded.Dir))
	require.Equal(t, filepath.Join(parent, "c0"), loaded.Dir)
	require.Equal(t, instance.Tag("c0"), loaded.Tag)
}

func TestLoad_RoundTrip(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != goosLinux {
		t.Skip("the node role is supported on linux only")
	}

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleNode
	cfg := config.DefaultNode()
	cfg.API.Hosts = []string{"203.0.113.1"}
	cfg.API.Port = 9443
	cfg.Country = "DE"
	cfg.ID = strings.Repeat("a", 64)
	cfg.Owner = (solana.PublicKey{1}).String()
	cfg.Service = wireguard.Name.String()
	serverCfg := wireguard.DefaultServerConfig()

	require.NoError(t, config.Save(home, file, cfg))
	require.NoError(t, config.SaveService(home, wireguard.Name, serverCfg))

	for _, name := range []string{"config.toml", "wireguard/config.toml"} {
		_, err := os.Stat(filepath.Join(home, name))
		require.NoError(t, err, name)
	}

	_, err := os.Stat(filepath.Join(home, "node"))
	require.ErrorIs(t, err, os.ErrNotExist)

	raw, err := os.ReadFile(filepath.Join(home, config.FileName)) //#nosec G304
	require.NoError(t, err)
	require.Contains(t, string(raw), "country = ")
	require.Contains(t, string(raw), "[rpc]")

	flags := newFlags(t)
	require.NoError(t, flags.Set("log.level", "debug"))

	loaded, err := config.Load("n0", home, flags)
	require.NoError(t, err)
	require.Equal(t, config.RoleNode, loaded.Role)
	require.EqualValues(t, 9443, loaded.Node.API.Port)
	require.Equal(t, "debug", loaded.File.Log.Level)
	require.Equal(t, "DE", loaded.Node.Country)
	require.EqualValues(t, 5, loaded.Node.Sessions.HeartbeatSeconds)
	require.EqualValues(t, 100_000_000, loaded.Node.Sessions.MinPrepayBytes)
	require.EqualValues(t, 60, loaded.Node.Sessions.MinPrepaySeconds)
	require.EqualValues(t, 3600, loaded.Node.Lease.MinPrepaySeconds)
	loadedCfg, ok := loaded.Services[wireguard.Name].(*wireguard.ServerConfig)
	require.True(t, ok)
	require.Equal(t, serverCfg.PrivateKey, loadedCfg.PrivateKey)
	require.Nil(t, loaded.Client)
}

func TestLoaded_Endpoint(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != goosWindows {
		linux := &config.Loaded{Dir: "/var/lib/sentinel-dvpn/c0", Tag: "c0"}
		// the service home: the socket is under /run (SELinux: var_run_t), as the socket unit has it
		require.Equal(t, "/run/sentinel-dvpn/c0/daemon.sock", config.LoadedEndpoint(linux, goosLinux))
		require.Equal(t, "/run/sentinel-dvpn/c0/daemon.sock", config.LoadedEndpoint(&config.Loaded{Dir: "/var/lib/sentinel-dvpn/c0/", Tag: "c0"}, goosLinux))

		darwin := &config.Loaded{Dir: "/Library/Application Support/sentinel-dvpn/c0", Tag: "c0"}
		require.Equal(
			t,
			"/Library/Application Support/sentinel-dvpn/c0/daemon.sock",
			config.LoadedEndpoint(darwin, goosDarwin),
		)

		home := &config.Loaded{Dir: "/tmp/home", Tag: "c0"}
		require.Equal(t, "/tmp/home/daemon.sock", config.LoadedEndpoint(home, goosLinux))
	}

	windows := &config.Loaded{Dir: `C:\ProgramData\sentinel-dvpn\c0`, Tag: "c0"}
	require.Equal(
		t,
		`\\.\pipe\ProtectedPrefix\Administrators\sentinel-dvpn\c0`,
		config.LoadedEndpoint(windows, goosWindows),
	)
}

func TestLoaded_Placement(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	cases := []struct {
		goos string
		name string
		root string
		svc  service.Name
		tag  instance.Tag
		want string
	}{
		{goos: goosLinux, name: "node wireguard on linux", svc: wireguard.Name, tag: "n0",
			want: "/etc/wireguard/sentinel-dvpn"},
		{goos: goosLinux, name: "node openvpn on linux", svc: openvpn.Name, tag: "n0",
			want: "/etc/openvpn/server/sentinel-dvpn"},
		{goos: goosDarwin, name: "client openvpn on darwin", svc: openvpn.Name, tag: "c0",
			want: "/etc/openvpn/client/sentinel-dvpn"},
		{goos: goosWindows, name: "client wireguard on windows", svc: wireguard.Name, tag: "c0",
			want: filepath.Join(dir, "wireguard")},
		{goos: goosLinux, name: "client wireguard under a tool root", root: "/tmp/tools", svc: wireguard.Name, tag: "c1",
			want: "/tmp/tools/etc/wireguard/sentinel-dvpn"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.tag.Role() == config.RoleNode && runtime.GOOS != goosLinux {
				t.Skip("the node role is supported on linux only")
			}

			if tc.goos != goosWindows && runtime.GOOS == goosWindows {
				t.Skip("tool folders use unix path separators")
			}

			l := &config.Loaded{Dir: dir, Role: tc.tag.Role(), Root: tc.root, Tag: tc.tag}
			require.Equal(t, service.Placement{
				Dir:       l.ServiceDir(tc.svc),
				Interface: "dvpn-" + string(tc.tag),
				Table:     "sentinel-dvpn-" + string(tc.tag),
				ToolDir:   tc.want,
			}, config.LoadedDerivePlacement(l, tc.svc, tc.goos))
		})
	}
}

func TestRoleSupported(t *testing.T) {
	t.Parallel()

	require.True(t, config.RoleSupported(config.RoleClient))
	require.False(t, config.RoleSupported("gateway"))

	for _, goos := range []string{goosDarwin, goosLinux, goosWindows} {
		require.True(t, config.RoleSupportedOn(config.RoleClient, goos), goos)
		require.Equal(t, goos == goosLinux, config.RoleSupportedOn(config.RoleNode, goos), goos)
		require.Equal(t, goos == goosLinux, config.RoleSupportedOn(config.RoleProvider, goos), goos)
		require.False(t, config.RoleSupportedOn("gateway", goos), goos)
	}
}

func TestValidateAPIHosts_HostLength(t *testing.T) {
	t.Parallel()

	host := strings.Repeat("a", 55) + "." + strings.Repeat("b", 56)
	for _, role := range []string{config.RoleNode, config.RoleProvider} {
		require.NoError(t, config.ValidateAPIHosts(role, []string{host}), role)

		err := config.ValidateAPIHosts(role, []string{host + "b"})
		require.ErrorIs(t, err, config.ErrInvalidAPIHost, role)

		err = config.ValidateAPIHosts(role, []string{"example.com", host + "b"})
		require.ErrorIs(t, err, config.ErrInvalidAPIHost, role)

		err = config.ValidateAPIHosts(role, []string{"example.com", "bad host"})
		require.ErrorIs(t, err, config.ErrInvalidAPIHost, role)
	}
}
