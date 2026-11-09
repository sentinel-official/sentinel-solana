package hysteria2_test

import (
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sentinel-official/sentinel-go-sdk/v3/hysteria2"
)

type tunRoutes struct {
	Tun struct {
		Address struct {
			IPv6 string `yaml:"ipv6"`
		} `yaml:"address"`
		Route struct {
			IPv4 []string `yaml:"ipv4"`
			IPv6 []string `yaml:"ipv6"`
		} `yaml:"route"`
	} `yaml:"tun"`
}

func readRoutes(t *testing.T, path string) tunRoutes {
	t.Helper()

	raw, err := os.ReadFile(path) //#nosec G304
	require.NoError(t, err)

	var doc tunRoutes
	require.NoError(t, yaml.Unmarshal(raw, &doc))

	return doc
}

func TestWriteConf_NoIPv6RoutesOnlyTheTunnelPrefix(t *testing.T) {
	t.Parallel()

	resp := hysteria2.PeerResponse{Auth: "00000000-0000-4000-8000-000000000001", Port: 443}
	addr := netip.MustParseAddr("203.0.113.1")

	path := filepath.Join(t.TempDir(), "client.yaml")
	require.NoError(t, hysteria2.WriteConfNoIPv6(path, addr, "dvpn-c0", resp))

	doc := readRoutes(t, path)
	require.Equal(t, []string{"0.0.0.0/0"}, doc.Tun.Route.IPv4, "IPv4 still goes through the tunnel")
	require.Equal(t, []string{"2001::ffff:ffff:ffff:fff0/126"}, doc.Tun.Route.IPv6,
		"only the tunnel's own IPv6 prefix is routed: an empty list would route all of IPv6")
	require.Equal(t, "2001::ffff:ffff:ffff:fff1/126", doc.Tun.Address.IPv6, "the routed prefix is the tunnel's own")

	path = filepath.Join(t.TempDir(), "client.yaml")
	require.NoError(t, hysteria2.WriteConf(path, addr, "dvpn-c0", resp))
	require.Equal(t, []string{"::/0"}, readRoutes(t, path).Tun.Route.IPv6, "the default still routes all of IPv6")
}
