package config_test

import (
	"runtime"
	"testing"

	"github.com/creasty/defaults"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
	"github.com/sentinel-official/sentinel-go-sdk/v3/wireguard"
)

func TestClient_PrepayLimits(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		change func(*config.Client)
		name   string
		valid  bool
	}{
		{change: func(cfg *config.Client) { cfg.MaxPrepayBytes = 0 }, name: "zero byte cap"},
		{change: func(cfg *config.Client) { cfg.MaxPrepaySeconds = 0 }, name: "zero second cap"},
		{change: func(cfg *config.Client) { cfg.NodeTimeoutSeconds = 0 }, name: "zero timeout"},
		{change: func(cfg *config.Client) { cfg.TolerancePercent = 0 }, name: "zero tolerance"},
		{change: func(cfg *config.Client) { cfg.TolerancePercent = 101 }, name: "tolerance above maximum"},
		{change: func(cfg *config.Client) { cfg.TolerancePercent = 100 }, name: "maximum tolerance", valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := config.DefaultClient()
			cfg.Owner = (solana.PublicKey{1}).String()
			cfg.Services = []string{"wireguard"}
			tc.change(&cfg)

			err := validate.Struct(cfg)

			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestClient_ZeroPrepay(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultClient()
	cfg.Owner = (solana.PublicKey{1}).String()
	cfg.PrepayBytes = 0
	cfg.PrepaySeconds = 0
	cfg.Services = []string{"wireguard"}

	require.NoError(t, validate.Struct(cfg))
}

func TestDefaultClient(t *testing.T) {
	t.Parallel()

	var want config.Client
	require.NoError(t, defaults.Set(&want))
	require.Equal(t, want, config.DefaultClient())
}

func TestDefaultClient_DNS(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultClient()
	require.Equal(t, config.DefaultDNSBackend(), cfg.Network.DNSBackend)
	require.Contains(t, config.DNSBackends(), cfg.Network.DNSBackend)
	require.Contains(t, config.DNSBackends(), config.DNSNone)

	switch runtime.GOOS {
	case "darwin":
		require.Equal(t, []string{config.DNSNetworksetup, config.DNSNone}, config.DNSBackends())
	case "windows":
		require.Equal(t, []string{config.DNSNetsh, config.DNSNone}, config.DNSBackends())
	default:
		require.Equal(t, []string{config.DNSResolvectl, config.DNSResolvconf, config.DNSNone}, config.DNSBackends())
	}
}

func TestDefaultClient_Prepay(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultClient()

	require.Equal(t, uint64(100_000_000), cfg.PrepayBytes)
	require.Equal(t, uint64(60), cfg.PrepaySeconds)
	require.Equal(t, uint64(268_435_456), cfg.MaxPrepayBytes)
	require.Equal(t, uint64(1_800), cfg.MaxPrepaySeconds)
	require.Equal(t, uint64(30), cfg.NodeTimeoutSeconds)
	require.Equal(t, uint8(5), cfg.TolerancePercent)
}

func TestLoad_DefaultsEmptyDNSBackend(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleClient
	cfg := config.DefaultClient()
	cfg.Owner = (solana.PublicKey{1}).String()
	cfg.Services = []string{"wireguard"}
	cfg.Network.DNSBackend = ""

	require.NoError(t, config.Save(home, file, cfg))

	serviceCfg, err := service.DefaultClientConfig(wireguard.Name)
	require.NoError(t, err)
	require.NoError(t, config.SaveService(home, wireguard.Name, serviceCfg))

	loaded, err := config.Load("c0", home, newFlags(t))
	require.NoError(t, err)
	require.Equal(t, config.DefaultDNSBackend(), loaded.Client.Network.DNSBackend)
}

func TestLoad_RejectsUnsupportedDNSBackend(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleClient
	cfg := config.DefaultClient()
	cfg.Owner = (solana.PublicKey{1}).String()
	cfg.Services = []string{"wireguard"}

	cfg.Network.DNSBackend = config.DNSNetsh
	if runtime.GOOS == "windows" {
		cfg.Network.DNSBackend = config.DNSNetworksetup
	}

	require.NoError(t, config.Save(home, file, cfg))

	_, err := config.Load("c0", home, newFlags(t))
	require.ErrorIs(t, err, config.ErrUnsupportedDNSBackend)
}
