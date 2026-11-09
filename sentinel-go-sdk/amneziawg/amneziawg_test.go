package amneziawg_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/amneziawg"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

func TestExtensions_Validate(t *testing.T) {
	t.Parallel()

	hexTag := "<b 0x" + strings.Repeat("ab", 750) + ">"
	oversized := hexTag + hexTag + "<r 1><b 0x" + strings.Repeat("ab", 537) + ">"
	require.Len(t, oversized, 4097)

	cases := []struct {
		mutate func(*amneziawg.Obfuscation)
		name   string
		valid  bool
	}{
		{mutate: func(*amneziawg.Obfuscation) {}, name: "no junk", valid: true},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = "<b 0xf6ab><t><r 32><rc 8><rd 1000>" }, name: "tags", valid: true},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = oversized }, name: "oversized"},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = strings.Repeat("<r 1>", 65) }, name: "too many tags"},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = "<b>" }, name: "bare bytes"},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = "<b 0xabc>" }, name: "odd hex"},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = "<b 0xzz>" }, name: "invalid hex"},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = "<c>" }, name: "unknown tag"},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = "<r 1001>" }, name: "random length above 1000"},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = "<t><t>" }, name: "repeated timestamp"},
		{mutate: func(o *amneziawg.Obfuscation) { o.I1 = "[Peer]" }, name: "section header"},
		{mutate: func(o *amneziawg.Obfuscation) { o.I5 = "<t> <r 1>" }, name: "i5 space"},
		{mutate: func(o *amneziawg.Obfuscation) { o.H2 = o.H1 }, name: "obfuscation rule"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := amneziawg.Extensions{Obfuscation: amneziawg.NewObfuscation()}
			tc.mutate(&e.Obfuscation)

			err := e.Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, validate.ErrInvalidConfig)
			}
		})
	}

	for _, vector := range []string{
		"\n",
		"\r\n",
		"\x00",
		`"`,
		"'",
		"]",
		"\n[Interface]\nPostUp = id\n",
		"\nscript-security 2\nup /bin/sh",
		"\nsocks5:\n  listen: 0.0.0.0:1080",
		`"},{"listen":"0.0.0.0"`,
	} {
		e := amneziawg.Extensions{Obfuscation: amneziawg.NewObfuscation()}
		e.Obfuscation.I1 = "<t>" + vector
		require.ErrorIs(t, e.Validate(), validate.ErrInvalidConfig, "i1 %q", vector)
	}
}

func TestNewClient_Registered(t *testing.T) {
	t.Parallel()

	tunnelBinary := "awg-quick"
	if runtime.GOOS == "windows" {
		tunnelBinary = "amneziawg"
	}

	require.Equal(t, []string{"awg", tunnelBinary}, service.Binaries(amneziawg.Name))

	if runtime.GOOS != "linux" {
		_, err := service.DefaultServerConfig(amneziawg.Name)
		require.ErrorIs(t, err, service.ErrUnknownName)
	}

	sdktest.RequireClientRegistered(t, amneziawg.Name, 1<<3, amneziawg.ClientConfig{})

	dir, err := service.ClientToolDir(amneziawg.Name)
	require.NoError(t, err)
	require.Equal(t, "/etc/amnezia/amneziawg/sentinel-dvpn", dir)
}

func TestNewObfuscation(t *testing.T) {
	t.Parallel()

	o := amneziawg.NewObfuscation()
	require.NoError(t, o.Validate())
	require.LessOrEqual(t, o.Jc, uint32(10))
	require.GreaterOrEqual(t, o.Jmin, uint32(64))
	require.NotEqual(t, o.S1+56, o.S2)

	seen := map[uint32]bool{o.H1: true, o.H2: true, o.H3: true, o.H4: true}
	require.Len(t, seen, 4)
}

func TestObfuscation_Validate(t *testing.T) {
	t.Parallel()

	cfg := amneziawg.DefaultServerConfig()
	cfg.Obfuscation.H2 = cfg.Obfuscation.H1
	err := cfg.Validate()
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.EqualError(t, err, "invalid configuration: magic headers must be distinct")

	cfg = amneziawg.DefaultServerConfig()
	cfg.Obfuscation.Jmin = 100
	cfg.Obfuscation.Jmax = 99
	err = cfg.Validate()
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.EqualError(t, err, "invalid configuration: jmax gtefield=Jmin")

	cfg = amneziawg.DefaultServerConfig()
	cfg.Obfuscation.S1 = 4
	cfg.Obfuscation.S2 = 60
	err = cfg.Validate()
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.EqualError(t, err, "invalid configuration: s2 must not equal s1+56")

	cfg = amneziawg.DefaultServerConfig()
	cfg.Obfuscation.H1 = 4
	err = cfg.Validate()
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.EqualError(t, err, "invalid configuration: h1 gt=4")

	o := amneziawg.NewObfuscation()
	o.Jc = 11
	err = o.Validate()
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.EqualError(t, err, "invalid configuration: jc lte=10")

	o = amneziawg.NewObfuscation()
	o.H4 = o.H3
	err = o.Validate()
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.EqualError(t, err, "invalid configuration: magic headers must be distinct")
}

func TestPreflightClient(t *testing.T) {
	t.Parallel()

	for _, check := range amneziawg.PreflightClient("darwin") {
		require.NotContains(t, check.Detail, "isn't supported", "AmneziaWG runs on macOS")
	}

	for _, check := range amneziawg.PreflightClient("linux") {
		require.NotEqual(t, "service_platform", check.Name)
	}
}

func TestServerConfig_Validate(t *testing.T) {
	t.Parallel()

	cfg := amneziawg.DefaultServerConfig()
	cfg.EgressInterface = "0123456789abcdef"
	require.ErrorIs(t, cfg.Validate(), validate.ErrInvalidConfig)

	cfg = amneziawg.DefaultServerConfig()
	cfg.Obfuscation.I1 = "[Peer]"
	require.ErrorIs(t, cfg.Validate(), validate.ErrInvalidConfig)

	cfg = amneziawg.DefaultServerConfig()
	cfg.EgressInterface = ""
	require.NoError(t, cfg.Validate())
}

func TestVariant_Templates_RendersKeepaliveSeconds(t *testing.T) {
	t.Parallel()

	data := map[string]any{
		"Addresses":        []string{"10.0.0.2/32"},
		"AllowedIPs":       "0.0.0.0/0",
		"Endpoint":         "203.0.113.1:12345",
		"KeepaliveSeconds": 25,
		"MTU":              1420,
		"Obfuscation":      amneziawg.Obfuscation{},
		"PrivateKey":       "private",
		"PublicKey":        "public",
	}

	raw, err := service.RenderBytes(amneziawg.Variant().Templates, "templates/client.conf.tmpl", data)
	require.NoError(t, err)
	require.Contains(t, string(raw), "PersistentKeepalive = 25\n")
}
