package config_test

import (
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpn/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
)

func TestAddFlags_DaemonEndpointHelp(t *testing.T) {
	t.Parallel()

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.AddFlags(fs)

	flag := fs.Lookup("daemon.endpoint")
	require.NotNil(t, flag)

	switch runtime.GOOS {
	case "darwin":
		require.Equal(
			t,
			"daemon endpoint (default /Library/Application Support/sentinel-dvpn/<tag>/daemon.sock)",
			flag.Usage,
		)
	case "windows":
		require.Equal(
			t,
			`daemon endpoint (default \\.\pipe\ProtectedPrefix\Administrators\sentinel-dvpn\<tag>)`,
			flag.Usage,
		)
	default:
		require.Equal(t, "daemon endpoint (default /run/sentinel-dvpn/<tag>/daemon.sock)", flag.Usage)
	}
}

func TestDetectDaemonEnv(t *testing.T) {
	t.Parallel()

	got, err := config.DetectDaemonEnv([]string{
		"PATH=/usr/bin",
		"SENTINEL_DVPND_PASSPHRASE=secret",
		"SENTINEL_DVPND_RPC_URL=http://127.0.0.1:8899",
		"SENTINEL_DVPND_TX_PAYMASTER_API_KEY=key",
		"SENTINEL_DVPN_KEY_NAME=owner",
	})
	require.NoError(t, err)
	require.Equal(t, []config.EnvMismatch{
		{Actual: "SENTINEL_DVPND_RPC_URL", Expected: "SENTINEL_DVPN_RPC_URL"},
		{Actual: "SENTINEL_DVPND_TX_PAYMASTER_API_KEY", Expected: "SENTINEL_DVPN_TX_PAYMASTER_API_KEY"},
	}, got)
}

func TestFile_JSONKeysMatchTOML(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(config.DefaultFile())
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Contains(t, got, "daemon")
	require.Contains(t, got, "output")
	require.Contains(t, got["key"], "name")
	require.Contains(t, got["log"], "format")
	require.Contains(t, got["rpc"], "ws_url")
	require.Contains(t, got["tx"], "priority_fee_lamports")
}

func TestLoad_EnvAndFlags(t *testing.T) {
	t.Setenv("SENTINEL_DVPN_KEY_NAME", "env-key")
	t.Setenv("SENTINEL_DVPN_DAEMON_ENDPOINT", "/run/env.sock")

	got, err := config.Load(t.TempDir(), nil)
	require.NoError(t, err)
	require.Equal(t, "env-key", got.Key.Name)
	require.Equal(t, "/run/env.sock", got.Daemon.Endpoint)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--key.name", "flag-key"}))

	got, err = config.Load(t.TempDir(), fs)
	require.NoError(t, err)
	require.Equal(t, "flag-key", got.Key.Name)
}

func TestLoad_PaymasterFlagsAndEnv(t *testing.T) {
	t.Setenv("SENTINEL_DVPN_TX_PAYMASTER_FALLBACK", "true")

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.AddFlags(fs)

	args := []string{
		"--tx.paymaster.url", "https://kora.example.com",
		"--tx.paymaster.api-key", "secret",
		"--tx.paymaster.fee-token", "11111111111111111111111111111111",
		"--tx.paymaster.max-fee-base-units", "5000",
	}
	require.NoError(t, fs.Parse(args))

	got, err := config.Load(t.TempDir(), fs)
	require.NoError(t, err)
	require.Equal(t, tx.PaymasterConfig{
		APIKey:          "secret",
		Fallback:        true,
		FeeToken:        "11111111111111111111111111111111",
		MaxFeeBaseUnits: 5000,
		URL:             "https://kora.example.com",
	}, got.Tx.Paymaster)
}

func TestLoad_PeerAllowPrivate(t *testing.T) {
	cases := []struct {
		args []string
		env  string
		file bool
		name string
		want bool
	}{
		{name: "default"},
		{file: true, name: "file", want: true},
		{env: "true", name: "env", want: true},
		{args: []string{"--peer.allow-private"}, env: "false", name: "flag over env", want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()

			if tc.file {
				file := config.DefaultFile()
				file.Peer.AllowPrivate = true
				require.NoError(t, config.Save(home, file))
			}

			if tc.env != "" {
				t.Setenv("SENTINEL_DVPN_PEER_ALLOW_PRIVATE", tc.env)
			}

			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			config.AddFlags(fs)
			require.NoError(t, fs.Parse(tc.args))

			got, err := config.Load(home, fs)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Peer.AllowPrivate)
		})
	}
}

func TestLoad_Precedence(t *testing.T) {
	t.Parallel()

	t.Run("file and flags", func(t *testing.T) {
		t.Parallel()

		home := t.TempDir()

		file := config.DefaultFile()
		file.Output = "table"
		require.NoError(t, config.Save(home, file))
		require.DirExists(t, filepath.Join(home, "keys"))

		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		config.AddFlags(fs)

		args := []string{"--tx.priority-fee-lamports", "42"}
		require.NoError(t, fs.Parse(args))

		got, err := config.Load(home, fs)
		require.NoError(t, err)
		require.Equal(t, "table", got.Output)
		require.EqualValues(t, 42, got.Tx.PriorityFeeLamports)
	})
	t.Run("kebab flag overrides snake key", func(t *testing.T) {
		t.Parallel()

		home := t.TempDir()

		file := config.DefaultFile()
		file.RPC.WSURL = "ws://file.example.com"
		require.NoError(t, config.Save(home, file))

		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		config.AddFlags(fs)

		args := []string{
			"--rpc.ws-url", "ws://flag.example.com",
			"--daemon.endpoint", "/run/sentinel-dvpnd.sock",
			"--tx.priority-fee-mode", "never",
		}
		require.NoError(t, fs.Parse(args))

		got, err := config.Load(home, fs)
		require.NoError(t, err)
		require.Equal(t, "ws://flag.example.com", got.RPC.WSURL)
		require.Equal(t, "/run/sentinel-dvpnd.sock", got.Daemon.Endpoint)
		require.Equal(t, "never", got.Tx.PriorityFeeMode)
	})
}
