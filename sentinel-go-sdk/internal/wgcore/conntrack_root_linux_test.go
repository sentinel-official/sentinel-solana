//go:build conntrackroot

package wgcore

import (
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
)

// Kernel check: needs CAP_NET_ADMIN and its own network namespace (a privileged container),
// because it writes the conntrack table. Run: go test -tags conntrackroot -run ConntrackRoot ./internal/wgcore/
func TestFlushConntrack_ConntrackRoot_DeletesOnlyTheReleasedAddress(t *testing.T) {
	v4 := func(s string) net.IP { return net.ParseIP(s).To4() }
	udp := func(src, dst string, sport, dport uint16, rsrc, rdst string) *netlink.ConntrackFlow {
		return &netlink.ConntrackFlow{
			FamilyType: netlink.FAMILY_V4,
			Forward:    netlink.IPTuple{SrcIP: v4(src), DstIP: v4(dst), SrcPort: sport, DstPort: dport, Protocol: 17},
			Reverse:    netlink.IPTuple{SrcIP: v4(rsrc), DstIP: v4(rdst), SrcPort: dport, DstPort: sport, Protocol: 17},
			TimeOut:    300,
		}
	}

	flows := []*netlink.ConntrackFlow{
		udp("10.212.1.2", "93.184.216.34", 40000, 443, "93.184.216.34", "203.0.113.5"), // old peer's NAT flow out
		udp("198.51.100.7", "10.212.1.2", 5000, 5000, "10.212.1.2", "198.51.100.7"),    // a flow to the old peer
		udp("10.212.1.3", "93.184.216.34", 40001, 443, "93.184.216.34", "203.0.113.5"), // control: another live peer
	}
	for _, f := range flows {
		require.NoError(t, netlink.ConntrackCreate(netlink.ConntrackTable, netlink.FAMILY_V4, f))
	}

	count := func(addr string) int {
		list, err := netlink.ConntrackTableList(netlink.ConntrackTable, netlink.FAMILY_V4)
		require.NoError(t, err)

		ip, n := net.ParseIP(addr), 0
		for _, f := range list {
			if f.Forward.SrcIP.Equal(ip) || f.Forward.DstIP.Equal(ip) {
				n++
			}
		}

		return n
	}

	require.Equal(t, 2, count("10.212.1.2"), "setup: two flows of the old peer")
	require.Equal(t, 1, count("10.212.1.3"), "setup: one flow of the other peer")

	deleted, err := flushConntrack([]netip.Addr{netip.MustParseAddr("10.212.1.2")})
	require.NoError(t, err)
	require.EqualValues(t, 2, deleted)

	require.Zero(t, count("10.212.1.2"), "no flow of the released address survives")
	require.Equal(t, 1, count("10.212.1.3"), "the other peer's flow is untouched")
}
