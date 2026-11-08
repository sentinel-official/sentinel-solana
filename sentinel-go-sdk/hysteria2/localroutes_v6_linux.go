package hysteria2

import (
	"bufio"
	"encoding/hex"
	"net/netip"
	"os"
	"strconv"
	"strings"
)

// localBypassV6 is localBypassV4 for IPv6: every non-default route on a physical
// interface (the LAN's prefix, a ULA like fd00::/64) plus the default gateway itself and
// link-local fe80::/10. Without it the TUN took the machine's own IPv6 network: with allow-LAN on,
// a LAN host over IPv6 was unreachable both ways while IPv4 worked.
func localBypassV6(tunIface string) []string {
	f, err := os.Open("/proc/net/ipv6_route")
	if err != nil {
		return []string{"fe80::/10"}
	}
	defer f.Close()

	return parseLocalBypassV6(bufio.NewScanner(f), tunIface)
}

// parseLocalBypassV6 reads /proc/net/ipv6_route: dest plen src srcplen nexthop metric refcnt use
// flags dev, addresses as 32 hex digits, lengths in hex. No header line.
func parseLocalBypassV6(sc *bufio.Scanner, tunIface string) []string {
	seen := map[string]bool{"fe80::/10": true}
	out := []string{"fe80::/10"}
	add := func(p netip.Prefix) {
		s := p.Masked().String()
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}

	multicast := netip.MustParsePrefix("ff00::/8")
	linkLocal := netip.MustParsePrefix("fe80::/10")

	for sc.Scan() {
		fl := strings.Fields(sc.Text())
		if len(fl) < 10 || tunnelLike(fl[9], tunIface) {
			continue
		}

		dst, okD := procAddr6(fl[0])
		gw, okG := procAddr6(fl[4])
		bits, err := strconv.ParseUint(fl[1], 16, 8)
		if !okD || !okG || err != nil || bits > 128 {
			continue
		}

		if bits == 0 { // the default route: keep only its gateway (unless link-local, already kept)
			if !gw.IsUnspecified() && !linkLocal.Contains(gw) {
				add(netip.PrefixFrom(gw, 128))
			}

			continue
		}

		if multicast.Contains(dst) || linkLocal.Contains(dst) || dst.IsLoopback() {
			continue
		}

		add(netip.PrefixFrom(dst, int(bits)))
	}

	return out
}

// procAddr6 reads /proc/net/ipv6_route's 32-hex-digit IPv6 (network order).
func procAddr6(h string) (netip.Addr, bool) {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 16 {
		return netip.Addr{}, false
	}

	return netip.AddrFrom16([16]byte(b)), true
}
