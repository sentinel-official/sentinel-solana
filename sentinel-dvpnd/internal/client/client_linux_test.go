package client_test

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
)

func TestRenderNftRuleset_AdmitsExactlyRPCAddrs(t *testing.T) {
	t.Parallel()

	nodeAddrs := []netip.Addr{netip.MustParseAddr("203.0.113.1"), netip.MustParseAddr("2001:db8::9")}
	rpcAndWSAddrs := []netip.AddrPort{
		netip.MustParseAddrPort("10.99.0.1:8899"),
		netip.MustParseAddrPort("10.99.0.1:8900"),
		netip.MustParseAddrPort("[2001:db8::7]:443"),
	}

	want := `table inet sentinel-dvpn-c0
delete table inet sentinel-dvpn-c0
table inet sentinel-dvpn-c0 {
  chain output {
    type filter hook output priority 0; policy drop;
    oif "lo" accept
    oifname "dvpn-c0" accept
    ip daddr 203.0.113.1 accept
    ip6 daddr 2001:db8::9 accept
    ip daddr 10.99.0.1 tcp dport 8899 accept
    ip daddr 10.99.0.1 tcp dport 8900 accept
    ip6 daddr 2001:db8::7 tcp dport 443 accept
    udp dport { 67, 68, 546, 547 } accept
    ip daddr 169.254.0.0/16 accept
    ip6 daddr fe80::/10 accept
    }
  chain input {
    type filter hook input priority 0; policy drop;
    iif "lo" accept
    ct state established,related accept
    iifname "dvpn-c0" accept
    udp dport { 68, 546 } accept
    ip saddr 169.254.0.0/16 accept
    ip6 saddr fe80::/10 accept
    icmpv6 type { nd-neighbor-solicit, nd-neighbor-advert, nd-router-advert } accept
    }
}
`
	require.Equal(t, want, client.RenderNftRuleset("sentinel-dvpn-c0", "dvpn-c0", nodeAddrs, rpcAndWSAddrs, false))
}

func TestRenderNftRuleset_BlocksLANByDefault(t *testing.T) {
	t.Parallel()

	nodeAddrs := []netip.Addr{netip.MustParseAddr("203.0.113.1")}
	rules := client.RenderNftRuleset("sentinel-dvpn-c0", "dvpn-c0", nodeAddrs, nil, false)

	require.Contains(t, rules, `oifname "dvpn-c0" accept`)
	require.Contains(t, rules, "ip daddr 203.0.113.1 accept")
	require.Contains(t, rules, "policy drop")
	require.NotContains(t, rules, "10.0.0.0/8")
}

func TestRenderNftRuleset_InboundChain(t *testing.T) {
	t.Parallel()

	off := client.RenderNftRuleset("sentinel-dvpn-c0", "dvpn-c0", nil, nil, false)
	require.Contains(t, off, "type filter hook input priority 0; policy drop;")
	require.Contains(t, off, "ct state established,related accept")
	require.NotContains(t, off, "ip saddr { 10.0.0.0/8")
	require.NotContains(t, off, "ip6 saddr fc00::/7")

	on := client.RenderNftRuleset("sentinel-dvpn-c0", "dvpn-c0", nil, nil, true)
	require.Contains(t, on, "ip saddr { 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16 } accept")
	require.Contains(t, on, "ip6 saddr fc00::/7 accept")
}
