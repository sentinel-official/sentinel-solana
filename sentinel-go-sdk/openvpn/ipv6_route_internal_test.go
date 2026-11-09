package openvpn

import (
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

func TestWriteConf_RedirectGatewayFollowsTunnelIPv6(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		on   bool
		want string
	}{
		{true, "redirect-gateway def1 ipv6 bypass-dhcp\n"},
		{false, "redirect-gateway def1 bypass-dhcp\n"},
	} {
		dir := t.TempDir()
		p := service.Placement{Dir: dir, Interface: "dvpn-c0", ToolDir: dir}
		c := &Client{conf: p.ToolPath(".conf"), iface: p.Interface, log: zerolog.Nop(), placement: p}
		c.SetTunnelIPv6(tc.on)

		conf, err := c.writeConf("203.0.113.1", PeerResponse{Port: 1194, Protocol: "udp"})
		require.NoError(t, err)

		raw, err := os.ReadFile(conf) //#nosec G304 -- test temp dir
		require.NoError(t, err)
		require.Contains(t, string(raw), tc.want)
		require.Equal(t, 1, strings.Count(string(raw), "redirect-gateway"))
	}
}
