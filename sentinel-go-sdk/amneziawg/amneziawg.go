package amneziawg

import (
	"context"
	"embed"
	"fmt"
	"regexp"
	"runtime"
	"strings"

	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/internal/wgcore"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

const Name service.Name = "amneziawg"

const (
	binary                     = "awg"
	defaultMTU                 = 1420
	maxJunkBytes               = 4096
	maxJunkTags                = 64
	protocolBit         uint32 = 1 << 3
	runDir                     = "/var/run/amneziawg"
	toolDir                    = "/etc/amnezia/amneziawg/" + instance.ProductName
	tunnelServicePrefix        = "AmneziaWGTunnel$"
	userspaceBinary            = "amneziawg-go"
)

var junkPattern = regexp.MustCompile(`^(<(b 0x([0-9a-fA-F]{2})+|t|(r|rc|rd) (1000|[1-9][0-9]{0,2}))>)*$`)

//nolint:gochecknoglobals
var lookupDirs = func() []string { return nil }

//go:embed templates/*.tmpl
var templates embed.FS

type ClientConfig struct{}

type Extensions struct {
	Obfuscation Obfuscation `json:"obfuscation"`
}

func (e Extensions) Validate() error {
	if err := e.Obfuscation.Validate(); err != nil {
		return err
	}

	o := e.Obfuscation

	return validateJunk(o.I1, o.I2, o.I3, o.I4, o.I5)
}

type Obfuscation struct {
	H1   uint32 `json:"h1"           toml:"h1"   validate:"gt=4"`
	H2   uint32 `json:"h2"           toml:"h2"   validate:"gt=4"`
	H3   uint32 `json:"h3"           toml:"h3"   validate:"gt=4"`
	H4   uint32 `json:"h4"           toml:"h4"   validate:"gt=4"`
	I1   string `json:"i1,omitempty" toml:"i1"`
	I2   string `json:"i2,omitempty" toml:"i2"`
	I3   string `json:"i3,omitempty" toml:"i3"`
	I4   string `json:"i4,omitempty" toml:"i4"`
	I5   string `json:"i5,omitempty" toml:"i5"`
	Jc   uint32 `json:"jc"           toml:"jc"   validate:"lte=10"`
	Jmax uint32 `json:"jmax"         toml:"jmax" validate:"lte=1024,gtefield=Jmin"`
	Jmin uint32 `json:"jmin"         toml:"jmin" validate:"gte=64"`
	S1   uint32 `json:"s1"           toml:"s1"   validate:"lte=64"`
	S2   uint32 `json:"s2"           toml:"s2"   validate:"lte=64"`
	S3   uint32 `json:"s3"           toml:"s3"   validate:"lte=64"`
	S4   uint32 `json:"s4"           toml:"s4"   validate:"lte=32"`
}

func NewObfuscation() Obfuscation {
	var o Obfuscation

	o.Jc = service.RandomUint32(1, 10)
	o.Jmin = service.RandomUint32(64, 512)
	o.Jmax = o.Jmin + service.RandomUint32(1, 512)
	o.S1 = service.RandomUint32(1, 64)
	o.S2 = service.RandomUint32(1, 64)

	for o.S1+56 == o.S2 {
		o.S2 = service.RandomUint32(1, 64)
	}

	o.S3 = service.RandomUint32(1, 64)
	o.S4 = service.RandomUint32(1, 32)

	seen := map[uint32]bool{}

	for _, h := range []*uint32{&o.H1, &o.H2, &o.H3, &o.H4} {
		for {
			v := service.RandomUint32(5, 1<<31)
			if !seen[v] {
				seen[v] = true
				*h = v

				break
			}
		}
	}

	return o
}

func (o *Obfuscation) Validate() error {
	if err := validate.Struct(o); err != nil {
		return err
	}

	return o.validateRules()
}

func (o *Obfuscation) validateRules() error {
	if o.S1+56 == o.S2 {
		return fmt.Errorf("%w: s2 must not equal s1+56", validate.ErrInvalidConfig)
	}

	headers := map[uint32]bool{o.H1: true, o.H2: true, o.H3: true, o.H4: true}
	if len(headers) != 4 {
		return fmt.Errorf("%w: magic headers must be distinct", validate.ErrInvalidConfig)
	}

	return nil
}

type ServerConfig struct {
	Addresses       []string    `toml:"addresses"        validate:"min=1,dive,cidr"`
	EgressInterface string      `toml:"egress_interface"`
	ListenPort      uint16      `toml:"listen_port"      validate:"gte=1024"`
	MTU             uint16      `toml:"mtu"              validate:"gte=1280"`
	Obfuscation     Obfuscation `toml:"obfuscation"`
	PrivateKey      string      `toml:"private_key"      validate:"required,base64"`
}

func (c ServerConfig) DerivePublicKey() (string, error) {
	pub, err := wgcore.DerivePublicKey(c.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("%w: private key: %w", validate.ErrInvalidConfig, err)
	}

	return pub, nil
}

func (c ServerConfig) Validate() error {
	if err := validate.Struct(c); err != nil {
		return err
	}

	if c.EgressInterface != "" {
		if err := service.ValidateInterfaceName(c.EgressInterface); err != nil {
			return err
		}
	}

	if _, err := service.ParsePrefixes(c.Addresses); err != nil {
		return err
	}

	if _, err := c.DerivePublicKey(); err != nil {
		return err
	}

	return Extensions{Obfuscation: c.Obfuscation}.Validate()
}

//nolint:gochecknoinits
func init() {
	driver := service.ClientDriver[ClientConfig]{
		Binaries:   []string{binary, tunnelBinary()},
		Bit:        protocolBit,
		Defaults:   DefaultClientConfig,
		LookupDirs: func() []string { return lookupDirs() },
		Metadata:   func() service.Validator { return &wgcore.Metadata[Extensions]{} },
		New:        NewClient,
		Preflight: func(_ context.Context, _ ClientConfig) []service.PreflightCheck {
			return preflightClient(runtime.GOOS)
		},
		ToolDir: toolDir,
	}
	service.RegisterClient(Name, driver)
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{}
}

func DefaultServerConfig() ServerConfig {
	key, err := wgcore.NewPrivateKey()
	if err != nil {
		panic(err)
	}

	return ServerConfig{
		Addresses:   []string{service.RandomPrefix().String()},
		ListenPort:  service.RandomPort(),
		MTU:         defaultMTU,
		Obfuscation: NewObfuscation(),
		PrivateKey:  key,
	}
}

func NewClient(_ ClientConfig, p service.Placement, log zerolog.Logger) (service.Client, error) {
	v := variant()

	if runtime.GOOS == "darwin" {
		bash, script, err := prepareDarwinQuick(darwinTools(), p)
		if err != nil {
			// awg-quick then runs as installed and fails at connect, as it did before.
			log.Warn().Err(err).Msg("preparing awg-quick for macOS")
		} else {
			v.TunnelBinary = bash
			v.TunnelPrefix = []string{script}
		}
	}

	return wgcore.NewClient[Extensions](v, p, log)
}

func preflightClient(goos string) []service.PreflightCheck {
	checks := wgcore.PreflightUserspace(variant(), goos)

	if goos == "darwin" {
		checks = append(checks, darwinQuickChecks(darwinTools())...)
	}

	return checks
}

func tunnelBinary() string {
	if runtime.GOOS == "windows" {
		return "amneziawg"
	}

	return "awg-quick"
}

func validateJunk(values ...string) error {
	for i, v := range values {
		if len(v) > maxJunkBytes {
			return fmt.Errorf("%w: i%d is %d bytes, want at most %d", validate.ErrInvalidConfig, i+1, len(v), maxJunkBytes)
		}

		if !junkPattern.MatchString(v) || strings.Count(v, "<") > maxJunkTags || strings.Count(v, "<t>") > 1 {
			return fmt.Errorf("%w: i%d is not a sequence of at most %d tags", validate.ErrInvalidConfig, i+1, maxJunkTags)
		}
	}

	return nil
}

func variant() wgcore.Variant {
	return wgcore.Variant{
		Binary:              binary,
		Name:                Name,
		RunDir:              runDir,
		Templates:           templates,
		TunnelBinary:        tunnelBinary(),
		TunnelServicePrefix: tunnelServicePrefix,
		UserspaceBinary:     userspaceBinary,
	}
}
