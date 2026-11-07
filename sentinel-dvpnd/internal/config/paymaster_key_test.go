package config_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
)

func savedClientConfig(t *testing.T, edit func(*config.File)) (string, string) {
	t.Helper()

	home := t.TempDir()
	file := config.DefaultFile(cli.NetworkLocalnet)
	file.Daemon.Role = config.RoleClient
	edit(&file)

	cfg := config.DefaultClient()
	cfg.Owner = (solana.PublicKey{1}).String()
	require.NoError(t, config.Save(home, file, cfg))

	path := filepath.Join(home, config.FileName)
	raw, err := os.ReadFile(path) //#nosec G304
	require.NoError(t, err)

	return home, string(raw)
}

// A line edit of the RPC URL (sed 's#^url = .*#…#') must not reach the paymaster.
func TestSave_OnlyTheRPCUsesTheURLKey(t *testing.T) {
	t.Parallel()

	_, raw := savedClientConfig(t, func(f *config.File) { f.Tx.Paymaster.URL = "http://127.0.0.1:8080" })

	urlLines := regexp.MustCompile(`(?m)^url = `).FindAllString(raw, -1)
	require.Len(t, urlLines, 1, "one url key in the generated config.toml, [rpc] url")
	require.Contains(t, raw, `endpoint = 'http://127.0.0.1:8080'`)
}

func TestLoad_PaymasterLegacyURLKey(t *testing.T) {
	home, raw := savedClientConfig(t, func(*config.File) {})
	path := filepath.Join(home, config.FileName)

	legacy := strings.Replace(raw, "[tx.paymaster]\n", "[tx.paymaster]\nurl = 'http://127.0.0.1:8080'\n", 1)
	require.NotEqual(t, raw, legacy)
	legacy = strings.Replace(legacy, "endpoint = ''\n", "", 1)
	require.NoError(t, os.WriteFile(path, []byte(legacy), 0o600)) //#nosec G703

	loaded, err := config.Load("c0", home, newFlags(t))
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:8080", loaded.File.Tx.Paymaster.URL, "an older config.toml keeps working")
}

func TestLoad_PaymasterLegacyURLConflict(t *testing.T) {
	home, raw := savedClientConfig(t, func(f *config.File) { f.Tx.Paymaster.URL = "http://127.0.0.1:8080" })
	path := filepath.Join(home, config.FileName)

	both := strings.Replace(raw, "[tx.paymaster]\n", "[tx.paymaster]\nurl = 'http://127.0.0.1:9090'\n", 1)
	require.NoError(t, os.WriteFile(path, []byte(both), 0o600)) //#nosec G703

	_, err := config.Load("c0", home, newFlags(t))
	require.ErrorIs(t, err, tx.ErrPaymasterURLConflict)
}
