package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
	"github.com/sentinel-official/sentinel-go-sdk/v3/version"
)

const FileName = "config.toml"

const (
	configFileMode = 0o600
	keysDirMode    = 0o700
	keysDirName    = "keys"

	daemonEnvPrefix = "SENTINEL_DVPND"
	envPrefix       = "SENTINEL_DVPN"
)

type Daemon struct {
	Endpoint string `json:"endpoint" toml:"endpoint"`
}

type EnvMismatch struct {
	Actual   string
	Expected string
}

type File struct {
	Daemon Daemon         `json:"daemon"  toml:"daemon"`
	Key    Key            `json:"key"     toml:"key"`
	Log    cli.LogConfig  `json:"log"     toml:"log"`
	Output string         `default:"json" json:"output" toml:"output" validate:"oneof=json table"`
	Peer   cli.PeerConfig `json:"peer"    toml:"peer"`
	RPC    cli.RPCConfig  `json:"rpc"     toml:"rpc"`
	Tx     tx.Config      `json:"tx"      toml:"tx"`
}

type Key struct {
	Name string `default:"owner" json:"name" toml:"name" validate:"required"`
}

func AddFlags(flags *pflag.FlagSet) {
	flags.String("daemon.endpoint", "", daemonEndpointHelp())
	flags.String("key.name", "", "signing key name under <home>/keys (default owner)")
	flags.String("log.format", "", "log format: console or json (default console)")
	flags.String("log.level", "", "log level (default warn)")
	flags.String("output", "", "output format: json or table (default json)")
	flags.Bool("peer.allow-private", false, "allow node and provider endpoints on private addresses")
	flags.String("rpc.url", "", "solana RPC URL (default for the build network)")
	flags.String("rpc.ws-url", "", "solana websocket URL (default for the build network)")
	flags.String(
		"tx.paymaster.api-key",
		"",
		"paymaster API key; prefer SENTINEL_DVPN_TX_PAYMASTER_API_KEY or config.toml, as flags show in ps (default none)",
	)
	flags.Bool("tx.paymaster.fallback", false, "pay fees and rent with the signing key when the paymaster fails")
	flags.String("tx.paymaster.fee-token", "", "paymaster fee token mint `address` (default the program mint)")
	flags.Uint64("tx.paymaster.max-fee-base-units", 0, "paymaster max fee `amount` in base units (default 0, no max)")
	flags.String("tx.paymaster.url", "", "kora paymaster json-rpc URL; its signer pays fees and rent (default none)")
	flags.Uint64("tx.priority-fee-lamports", 0, "fixed priority fee `amount` in lamports (default 0)")
	flags.String("tx.priority-fee-mode", "", "priority fee mode: auto, fixed, or never (default auto)")
}

func DefaultFile() File {
	cfg := File{
		Key: Key{Name: "owner"},
		Log: cli.LogConfig{
			Format: cli.LogFormatConsole,
			Level:  "warn",
		},
		Output: "json",
		RPC:    cli.DefaultRPCConfig(version.Network),
		Tx:     tx.DefaultConfig(),
	}

	return cfg
}

func DetectDaemonEnv(environ []string) ([]EnvMismatch, error) {
	settings := viper.New()
	if err := setDefaults(settings, DefaultFile()); err != nil {
		return nil, err
	}

	names := make(map[string]bool)
	for _, key := range settings.AllKeys() {
		names[strings.ToUpper(strings.ReplaceAll(key, ".", "_"))] = true
	}

	var mismatches []EnvMismatch

	for _, entry := range environ {
		variable, _, _ := strings.Cut(entry, "=")

		name, ok := strings.CutPrefix(variable, daemonEnvPrefix+"_")
		if !ok || !names[name] {
			continue
		}

		mismatches = append(mismatches, EnvMismatch{
			Actual:   variable,
			Expected: envPrefix + "_" + name,
		})
	}

	slices.SortFunc(mismatches, func(a, b EnvMismatch) int { return strings.Compare(a.Actual, b.Actual) })

	return mismatches, nil
}

func KeysDir(home string) string {
	return filepath.Join(home, keysDirName)
}

func Load(home string, flags *pflag.FlagSet) (*File, error) {
	settings := viper.New()
	settings.SetEnvPrefix(envPrefix)
	settings.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	settings.AutomaticEnv()
	settings.SetConfigFile(filepath.Join(home, FileName))

	cfg := DefaultFile()
	if err := setDefaults(settings, cfg); err != nil {
		return nil, err
	}

	if err := settings.ReadInConfig(); err != nil && !os.IsNotExist(err) {
		if _, ok := errors.AsType[viper.ConfigFileNotFoundError](err); !ok {
			return nil, fmt.Errorf("reading %s: %w", settings.ConfigFileUsed(), err)
		}
	}

	if flags != nil {
		if err := bindFlags(settings, flags); err != nil {
			return nil, err
		}
	}

	useTOMLTags := func(decoder *mapstructure.DecoderConfig) { decoder.TagName = "toml" }

	if err := settings.Unmarshal(&cfg, viper.DecoderConfigOption(useTOMLTags)); err != nil {
		return nil, fmt.Errorf("decoding config: %w", err)
	}

	if err := validate.Struct(&cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

func Save(home string, cfg File) error {
	if err := os.MkdirAll(KeysDir(home), keysDirMode); err != nil {
		return fmt.Errorf("creating keys directory: %w", err)
	}

	raw, err := toml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.WriteFile(filepath.Join(home, FileName), raw, configFileMode); err != nil {
		return fmt.Errorf("writing %s: %w", FileName, err)
	}

	return nil
}

func bindFlags(settings *viper.Viper, flags *pflag.FlagSet) error {
	var bindErr error

	flags.VisitAll(func(flag *pflag.Flag) {
		if bindErr != nil {
			return
		}

		if err := settings.BindPFlag(strings.ReplaceAll(flag.Name, "-", "_"), flag); err != nil {
			bindErr = fmt.Errorf("binding flag %s: %w", flag.Name, err)
		}
	})

	return bindErr
}

func daemonEndpointHelp() string {
	switch runtime.GOOS {
	case "darwin":
		return "daemon endpoint (default /Library/Application Support/sentinel-dvpn/<tag>/daemon.sock)"
	case "windows":
		return `daemon endpoint (default \\.\pipe\ProtectedPrefix\Administrators\sentinel-dvpn\<tag>)`
	default:
		return "daemon endpoint (default /run/sentinel-dvpn/<tag>/daemon.sock)"
	}
}

func setDefaults(settings *viper.Viper, cfg File) error {
	raw, err := toml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling defaults: %w", err)
	}

	defaults := viper.New()
	defaults.SetConfigType("toml")

	if err := defaults.ReadConfig(bytes.NewReader(raw)); err != nil {
		return fmt.Errorf("loading defaults: %w", err)
	}

	for _, name := range defaults.AllKeys() {
		settings.SetDefault(name, defaults.Get(name))
	}

	return nil
}
