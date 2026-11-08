package hysteria2

import (
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A GCE VM (/32 address, gateway on a link route) and a home LAN with a running WireGuard tunnel.
func TestParseLocalBypassV4(t *testing.T) {
	t.Parallel()

	gce := "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n" +
		"ens4\t00000000\t0100800A\t0003\t0\t0\t100\t00000000\t0\t0\t0\n" +
		"ens4\t0100800A\t00000000\t0005\t0\t0\t100\tFFFFFFFF\t0\t0\t0\n" +
		"dvpn-hy2-client\t00000000\t00000000\t0001\t0\t0\t0\t00000080\t0\t0\t0\n"
	require.Equal(t, []string{"169.254.0.0/16", "10.128.0.1/32"},
		parseLocalBypassV4(bufio.NewScanner(strings.NewReader(gce)), "dvpn-hy2-client"))

	home := "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n" +
		"wlp2s0\t00000000\t0101A8C0\t0003\t0\t0\t600\t00000000\t0\t0\t0\n" +
		"wlp2s0\t0001A8C0\t00000000\t0001\t0\t0\t600\t00FFFFFF\t0\t0\t0\n" +
		"wg0\t0000000A\t00000000\t0001\t0\t0\t0\t000000FF\t0\t0\t0\n" +
		"docker0\t000011AC\t00000000\t0001\t0\t0\t0\t0000FFFF\t0\t0\t0\n"
	require.Equal(t, []string{"169.254.0.0/16", "192.168.1.1/32", "192.168.1.0/24", "172.17.0.0/16"},
		parseLocalBypassV4(bufio.NewScanner(strings.NewReader(home)), ""))
}
