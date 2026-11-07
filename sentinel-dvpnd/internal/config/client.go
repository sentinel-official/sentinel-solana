package config

const (
	BasisPerGB        = "per_gb"
	BasisPerHour      = "per_hour"
	DNSNetsh          = "netsh"
	DNSNetworksetup   = "networksetup"
	DNSNone           = "none"
	DNSResolvconf     = "resolvconf"
	DNSResolvectl     = "resolvectl"
	KillSwitchOff     = "off"
	KillSwitchOn      = "on"
	KillSwitchPersist = "persist"
)

type Client struct {
	Basis              string   `default:"per_gb"     toml:"basis"                      validate:"oneof=per_gb per_hour"`
	EscrowBaseUnits    uint64   `default:"1000000000" toml:"escrow_base_units"          validate:"gt=0"`
	Key                Key      `toml:"key"`
	MaxPrepayBytes     uint64   `default:"268435456"  toml:"max_prepay_bytes"           validate:"gt=0"`
	MaxPrepaySeconds   uint64   `default:"1800"       toml:"max_prepay_seconds"         validate:"gt=0"`
	Network            Network  `toml:"network"`
	NodeTimeoutSeconds uint64   `default:"30"         toml:"node_timeout_seconds"       validate:"gt=0"`
	Owner              string   `toml:"owner"         validate:"required"`
	PrepayBytes        uint64   `default:"100000000"  toml:"prepay_bytes"`
	PrepaySeconds      uint64   `default:"60"         toml:"prepay_seconds"`
	Services           []string `toml:"services"      validate:"required,dive,required"`
	TolerancePercent   uint8    `default:"5"          toml:"tolerance_percent"          validate:"gt=0,lte=100"`
}

func (c *Client) SetDefaults() {
	if c.Network.DNSBackend == "" {
		c.Network.DNSBackend = DefaultDNSBackend()
	}

	if c.Key.Name == "" {
		c.Key.Name = operatorKeyName
	}
}

type Network struct {
	AllowLAN   bool     `default:"false"                     toml:"allow_lan"`
	DNSBackend string   `toml:"dns_backend"`
	DNSServers []string `default:"[\"1.1.1.1\",\"1.0.0.1\"]" toml:"dns_servers"`
	KillSwitch string   `default:"on"                        toml:"kill_switch" validate:"oneof=off on persist"`
}

func DefaultClient() Client {
	return Client{
		Basis:            BasisPerGB,
		EscrowBaseUnits:  1_000_000_000,
		Key:              Key{Name: operatorKeyName},
		MaxPrepayBytes:   268_435_456,
		MaxPrepaySeconds: 1_800,
		Network: Network{
			DNSBackend: DefaultDNSBackend(),
			DNSServers: []string{"1.1.1.1", "1.0.0.1"},
			KillSwitch: KillSwitchOn,
		},
		NodeTimeoutSeconds: 30,
		PrepayBytes:        100_000_000,
		PrepaySeconds:      60,
		TolerancePercent:   5,
	}
}
