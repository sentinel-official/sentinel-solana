package hysteria2

import (
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The rendered client config keeps every local range off the tunnel, beside the server's own
// address. Without it the TUN routes the LAN, so allow_lan's firewall rule has nothing to admit.
func TestWriteConf_ExcludesTheMachinesOwnNetworks(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "client.yaml")
	server := netip.MustParseAddr("203.0.113.7")
	require.NoError(t, writeConf(path, server, "dvpn-test", PeerResponse{Auth: "a", Port: 443, TLSPin: "p"}, false))

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	conf := string(b)

	v4 := section(conf, "ipv4Exclude:")
	require.Contains(t, v4, `- "203.0.113.7/32"`, "the server stays off the tunnel")

	local := localBypassV4("dvpn-test")
	require.Contains(t, local, "169.254.0.0/16")
	for _, p := range local {
		require.Contains(t, v4, `- "`+p+`"`, "local IPv4 range %s must stay off the tunnel", p)
	}

	for _, p := range localBypassV6("dvpn-test") {
		require.Contains(t, section(conf, "ipv6Exclude:"), `- "`+p+`"`, "local IPv6 range %s must stay off the tunnel", p)
	}

	require.Contains(t, conf, "      - 0.0.0.0/0\n", "the default route still goes through the tunnel")
}

// section returns the list under key, up to the next line that is not a list item.
func section(conf, key string) string {
	i := strings.Index(conf, key)
	if i < 0 {
		return ""
	}

	var out []string
	for _, l := range strings.Split(conf[i+len(key):], "\n")[1:] {
		if !strings.HasPrefix(strings.TrimSpace(l), "- ") {
			break
		}
		out = append(out, l)
	}

	return strings.Join(out, "\n")
}
