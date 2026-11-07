package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"

	"github.com/creasty/defaults"
	"github.com/go-playground/validator/v10"
	"github.com/go-viper/mapstructure/v2"
	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

const (
	FileName      = "config.toml"
	PassphraseEnv = "SENTINEL_DVPND_PASSPHRASE" //#nosec G101
	RoleClient    = "client"
	RoleNode      = "node"
	RoleProvider  = "provider"
)

const (
	cliEnvPrefix    = "SENTINEL_DVPN"
	configDirMode   = 0o700
	configFileMode  = 0o600
	defaultLogLevel = "info"
	envPrefix       = "SENTINEL_DVPND"
	keysDirName     = "keys"
	maxAPIHostBytes = 112
)

//nolint:gochecknoglobals
var (
	ErrInvalidAPIHost = errors.New(
		"want a host name or an IP address, without brackets or a port (the port is api.port)",
	)
	ErrInvalidRole           = errors.New("daemon.role must be client, node, or provider")
	ErrRoleMismatch          = errors.New("daemon.role does not match the instance")
	ErrUnsupportedDNSBackend = errors.New("network.dns_backend is not supported on this platform")
	ErrUnsupportedRole       = errors.New("role is supported on linux only")
	Roles                    = []string{RoleClient, RoleNode, RoleProvider}
)

type API struct {
	Hosts []string `toml:"hosts"   validate:"min=1,max=4,dive,required"`
	Port  uint16   `default:"8443" toml:"port"                          validate:"gte=1024"`
}

type Daemon struct {
	Role string `toml:"role"`
}

type EnvMismatch struct {
	Actual   string
	Expected string
}

type File struct {
	Daemon  Daemon         `toml:"daemon"`
	Log     cli.LogConfig  `toml:"log"`
	Metrics Metrics        `toml:"metrics"`
	Peer    cli.PeerConfig `toml:"peer"`
	RPC     cli.RPCConfig  `toml:"rpc"`
	Tx      tx.Config      `toml:"tx"`
}

type Key struct {
	Name           string `toml:"name"            validate:"required"`
	PassphraseFile string `toml:"passphrase_file"`
}

type Loaded struct {
	Client   *Client
	Dir      string
	File     File
	Node     *Node
	Provider *Provider
	Role     string
	Root     string
	Services map[service.Name]any
	Tag      instance.Tag
}

func (l *Loaded) Endpoint() string { return l.endpoint(runtime.GOOS) }

func (l *Loaded) KeyPath() (string, error) {
	return cli.KeyPath(filepath.Join(l.Dir, keysDirName), l.key().Name)
}

func (l *Loaded) LoadPassphrase() (string, error) {
	path := l.key().PassphraseFile
	if path == "" {
		return os.Getenv(PassphraseEnv), nil
	}

	raw, err := os.ReadFile(path) //#nosec G304
	if err != nil {
		return "", fmt.Errorf("reading passphrase file: %w", err)
	}

	return strings.TrimRight(string(raw), "\r\n"), nil
}

func (l *Loaded) Placement(name service.Name) service.Placement {
	return l.derivePlacement(name, runtime.GOOS)
}

func (l *Loaded) ServiceDir(name service.Name) string {
	return filepath.Join(l.Dir, name.String())
}

func (l *Loaded) derivePlacement(name service.Name, goos string) service.Placement {
	p := service.Placement{
		Dir:       l.ServiceDir(name),
		Interface: l.Tag.Interface(),
		Table:     l.Tag.ObjectName(),
	}

	if goos == "windows" {
		p.ToolDir = p.Dir

		return p
	}

	toolDir := service.ClientToolDir
	if l.Role == RoleNode {
		toolDir = service.ServerToolDir
	}

	if dir, err := toolDir(name); err == nil {
		p.ToolDir = filepath.Join(l.Root, dir)
	}

	return p
}

func (l *Loaded) endpoint(goos string) string {
	if goos == "windows" {
		return l.Tag.Endpoint(goos)
	}

	// The default home is the service's: the socket unit, the CLI and the daemon agree on the
	// instance endpoint. A home given with --home keeps its socket inside it.
	if goos == "linux" && filepath.Clean(l.Dir) == l.Tag.StateDir(goos) {
		return l.Tag.Endpoint(goos)
	}

	return filepath.Join(l.Dir, daemonv1.SocketFileName)
}

func (l *Loaded) key() Key {
	switch l.Role {
	case RoleClient:
		return l.Client.Key
	case RoleNode:
		return l.Node.Key
	case RoleProvider:
		return l.Provider.Key
	default:
		return Key{}
	}
}

type Metrics struct {
	Enabled bool   `toml:"enabled"`
	Listen  string `default:"127.0.0.1:9100" toml:"listen" validate:"hostname_port"`
}

func AccessGroup(tag instance.Tag, dir string) string {
	if dir != tag.StateDir(runtime.GOOS) {
		return ""
	}

	return instance.GroupName
}

func AddFlags(flags *pflag.FlagSet) {
	flags.String("log.format", "", "log format: json or console")
	flags.String("log.level", "", "log level")
	flags.Bool("peer.allow-private", false, "allow node and provider endpoints on private addresses")
	flags.String("rpc.url", "", "solana json-rpc url")
	flags.String("rpc.ws-url", "", "solana websocket url")
}

func DefaultDNSBackend() string { return defaultDNSBackend(runtime.GOOS) }

func DefaultFile(network string) File {
	return File{
		Log:     defaultLog(),
		Metrics: Metrics{Listen: "127.0.0.1:9100"},
		RPC:     cli.DefaultRPCConfig(network),
		Tx:      tx.DefaultConfig(),
	}
}

func DetectCLIEnv(environ []string) ([]EnvMismatch, error) {
	targets := []any{File{}, Client{}, Node{}, Provider{}}

	for _, name := range service.Names() {
		for _, empty := range []func(service.Name) (any, error){service.EmptyClientConfig, service.EmptyServerConfig} {
			cfg, err := empty(name)
			if errors.Is(err, service.ErrUnknownName) {
				continue
			}

			if err != nil {
				return nil, fmt.Errorf("resolving config for %s: %w", name, err)
			}

			targets = append(targets, cfg)
		}
	}

	settings := viper.New()
	for _, target := range targets {
		bindEnvKeys(settings, target)
	}

	names := map[string]bool{strings.TrimPrefix(PassphraseEnv, envPrefix+"_"): true}
	for _, key := range settings.AllKeys() {
		names[strings.ToUpper(strings.ReplaceAll(key, ".", "_"))] = true
	}

	var mismatches []EnvMismatch

	for _, entry := range environ {
		variable, _, _ := strings.Cut(entry, "=")

		name, ok := strings.CutPrefix(variable, cliEnvPrefix+"_")
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

func Dir(tag instance.Tag, home string) (string, error) {
	if home == "" {
		return tag.StateDir(runtime.GOOS), nil
	}

	dir, err := filepath.Abs(home)
	if err != nil {
		return "", fmt.Errorf("resolving home: %w", err)
	}

	return dir, nil
}

func DNSBackends() []string { return dnsBackends(runtime.GOOS) }

func Find(tag instance.Tag, home string) (string, error) {
	dir, err := Dir(tag, home)
	if err != nil {
		return "", err
	}

	_, err = os.Stat(filepath.Join(dir, FileName))
	if err == nil {
		return dir, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("finding instance: %w", err)
	}

	available, err := instance.List(filepath.Dir(dir))
	if err != nil {
		return "", err
	}

	return "", instance.NewNotFoundError(tag, available)
}

func Load(tag instance.Tag, home string, flags *pflag.FlagSet) (*Loaded, error) {
	dir, err := Find(tag, home)
	if err != nil {
		return nil, err
	}

	file := File{
		Log: defaultLog(),
		Tx:  tx.DefaultConfig(),
	}

	v := newViper(flags)
	if err := readInTOML(v, filepath.Join(dir, FileName), &file); err != nil {
		return nil, err
	}

	if role := v.GetString("daemon.role"); role != tag.Role() {
		return nil, fmt.Errorf("%w %s: %q", ErrRoleMismatch, tag, role)
	}

	if err := decodeAndValidate(v, &file); err != nil {
		return nil, err
	}

	if !RoleSupported(file.Daemon.Role) {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedRole, file.Daemon.Role)
	}

	loaded := &Loaded{
		Dir:  dir,
		File: file,
		Role: file.Daemon.Role,
		Tag:  tag,
	}
	switch loaded.Role {
	case RoleClient:
		if err := loadClient(loaded); err != nil {
			return nil, err
		}
	case RoleNode:
		if err := loadNode(loaded); err != nil {
			return nil, err
		}
	case RoleProvider:
		if err := loadProvider(loaded); err != nil {
			return nil, err
		}
	}

	return loaded, nil
}

func Paths(role string) []string {
	paths := []string{FileName}
	if role == RoleProvider {
		return paths
	}

	for _, name := range service.Names() {
		paths = append(paths, servicePath(name))
	}

	return paths
}

func RoleSupported(role string) bool { return roleSupported(role, runtime.GOOS) }

func Save(dir string, file File, roleCfg any) error {
	values := []any{file}
	if roleCfg != nil {
		values = []any{roleCfg, file}
	}

	return writeTOML(filepath.Join(dir, FileName), values...)
}

func SaveService(dir string, name service.Name, cfg any) error {
	return writeTOML(filepath.Join(dir, servicePath(name)), cfg)
}

func ValidateAPIHosts(role string, hosts []string) error {
	v := validator.New()

	for i, host := range hosts {
		if len(host) > maxAPIHostBytes {
			return fmt.Errorf("%s.api.hosts[%d] = %q: %w", role, i, host, ErrInvalidAPIHost)
		}

		if err := v.Var(host, "ip|hostname_rfc1123"); err != nil {
			return fmt.Errorf("%s.api.hosts[%d] = %q: %w", role, i, host, ErrInvalidAPIHost)
		}
	}

	return nil
}

func bindEnvKeys(v *viper.Viper, target any) {
	var walk func(reflect.Type, string)

	walk = func(typ reflect.Type, prefix string) {
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}

		if typ.Kind() != reflect.Struct {
			return
		}

		for field := range typ.Fields() {
			name, _, _ := strings.Cut(field.Tag.Get("toml"), ",")
			if name == "" || name == "-" {
				continue
			}

			key := name
			if prefix != "" {
				key = prefix + "." + name
			}

			nested := field.Type
			for nested.Kind() == reflect.Pointer {
				nested = nested.Elem()
			}

			if nested.Kind() == reflect.Struct {
				walk(nested, key)

				continue
			}

			_ = v.BindEnv(key)
		}
	}

	walk(reflect.TypeOf(target), "")
}

func bindFlags(v *viper.Viper, flags *pflag.FlagSet) {
	flags.VisitAll(func(flag *pflag.Flag) {
		_ = v.BindPFlag(strings.ReplaceAll(flag.Name, "-", "_"), flag)
	})
}

func decodeAndValidate(v *viper.Viper, target any) error {
	if err := setDefaults(target); err != nil {
		return err
	}

	withTOMLTag := func(cfg *mapstructure.DecoderConfig) { cfg.TagName = "toml" }
	if err := v.Unmarshal(target, viper.DecoderConfigOption(withTOMLTag)); err != nil {
		return fmt.Errorf("decoding %s: %w", v.ConfigFileUsed(), err)
	}

	if err := validate.Struct(target); err != nil {
		return fmt.Errorf("validating %s: %w", v.ConfigFileUsed(), err)
	}

	return nil
}

func defaultDNSBackend(goos string) string {
	switch goos {
	case "darwin":
		return DNSNetworksetup
	case "windows":
		return DNSNetsh
	default:
		return DNSResolvectl
	}
}

func defaultLog() cli.LogConfig {
	return cli.LogConfig{
		Format: cli.LogFormatJSON,
		Level:  defaultLogLevel,
	}
}

func dnsBackends(goos string) []string {
	switch goos {
	case "darwin":
		return []string{DNSNetworksetup, DNSNone}
	case "windows":
		return []string{DNSNetsh, DNSNone}
	default:
		return []string{DNSResolvectl, DNSResolvconf, DNSNone}
	}
}

func loadClient(loaded *Loaded) error {
	var cfg Client
	if err := readTOML(newViper(nil), filepath.Join(loaded.Dir, FileName), &cfg); err != nil {
		return err
	}

	cfg.SetDefaults()

	if !slices.Contains(DNSBackends(), cfg.Network.DNSBackend) {
		return fmt.Errorf("%w: %q, want one of %v", ErrUnsupportedDNSBackend, cfg.Network.DNSBackend, DNSBackends())
	}

	services, err := loadClientServices(loaded.Dir, cfg.Services)
	if err != nil {
		return err
	}

	loaded.Client = &cfg
	loaded.Services = services

	return nil
}

func loadClientServices(dir string, names []string) (map[service.Name]any, error) {
	services := make(map[service.Name]any, len(names))
	for _, text := range names {
		name, err := service.ParseName(text)
		if err != nil {
			return nil, fmt.Errorf("parsing client service %s: %w", text, err)
		}

		cfg, err := service.EmptyClientConfig(name)
		if err != nil {
			return nil, fmt.Errorf("resolving client config for %s: %w", name, err)
		}

		if err := readTOML(newViper(nil), filepath.Join(dir, name.String(), FileName), cfg); err != nil {
			return nil, err
		}

		services[name] = cfg
	}

	return services, nil
}

func loadNode(loaded *Loaded) error {
	var cfg Node

	path := filepath.Join(loaded.Dir, FileName)
	if err := readTOML(newViper(nil), path, &cfg); err != nil {
		return err
	}

	if err := ValidateAPIHosts(loaded.Role, cfg.API.Hosts); err != nil {
		return fmt.Errorf("validating %s: %w", path, err)
	}

	services, err := loadNodeService(loaded.Dir, cfg.Service)
	if err != nil {
		return err
	}

	loaded.Node = &cfg
	loaded.Services = services

	return nil
}

func loadNodeService(dir, text string) (map[service.Name]any, error) {
	name, err := service.ParseName(text)
	if err != nil {
		return nil, fmt.Errorf("parsing node service: %w", err)
	}

	cfg, err := service.EmptyServerConfig(name)
	if err != nil {
		return nil, fmt.Errorf("resolving server config for %s: %w", name, err)
	}

	path := filepath.Join(dir, name.String(), FileName)
	if err := readTOML(newViper(nil), path, cfg); err != nil {
		return nil, err
	}

	if v, ok := cfg.(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return nil, fmt.Errorf("validating %s: %w", path, err)
		}
	}

	return map[service.Name]any{name: cfg}, nil
}

func loadProvider(loaded *Loaded) error {
	var cfg Provider

	path := filepath.Join(loaded.Dir, FileName)
	if err := readTOML(newViper(nil), path, &cfg); err != nil {
		return err
	}

	if err := ValidateAPIHosts(loaded.Role, cfg.API.Hosts); err != nil {
		return fmt.Errorf("validating %s: %w", path, err)
	}

	loaded.Provider = &cfg

	return nil
}

func newViper(flags *pflag.FlagSet) *viper.Viper {
	v := viper.New()
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if flags != nil {
		bindFlags(v, flags)
	}

	return v
}

func readInTOML(v *viper.Viper, path string, target any) error {
	v.SetConfigFile(path)

	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}

	bindEnvKeys(v, target)

	return nil
}

func readTOML(v *viper.Viper, path string, target any) error {
	if err := readInTOML(v, path, target); err != nil {
		return err
	}

	return decodeAndValidate(v, target)
}

func roleSupported(role, goos string) bool {
	if goos == "linux" {
		return slices.Contains(Roles, role)
	}

	return role == RoleClient
}

func servicePath(name service.Name) string {
	return filepath.Join(name.String(), FileName)
}

func setDefaults(value any) error {
	if err := defaults.Set(value); err != nil {
		return fmt.Errorf("applying defaults: %w", err)
	}

	return nil
}

func writeTOML(path string, values ...any) error {
	if err := os.MkdirAll(filepath.Dir(path), configDirMode); err != nil {
		return fmt.Errorf("creating %s: %w", filepath.Dir(path), err)
	}

	var raw []byte

	for _, value := range values {
		part, err := toml.Marshal(value)
		if err != nil {
			return fmt.Errorf("encoding %s: %w", path, err)
		}

		if len(raw) > 0 {
			raw = append(raw, '\n')
		}

		raw = append(raw, part...)
	}

	if err := os.WriteFile(path, raw, configFileMode); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}

	return nil
}
