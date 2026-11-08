package hysteria2

import (
	"bufio"
	byteorder "encoding/binary"
	"encoding/hex"
	"net/netip"
	"os"
	"strings"
)

// localBypassV4 lists the IPv4 ranges a full Hysteria2 tunnel must leave on the machine's own
// network: every non-default route of the main table on a physical interface (the LAN, the
// gateway's link route) plus the default gateway itself and link-local 169.254.0.0/16. This is
// what WireGuard's `suppress_prefixlength 0` keeps for free. Without it the TUN takes the
// gateway too: replies on the NIC fail the reverse-path check (inbound SSH, the LAN, DHCP
// renewals die) and, on a cloud VM, the metadata server answers from the node's side.
func localBypassV4(tunIface string) []string {
	f, err := os.Open("/proc/net/route")
	if err != nil {
		return []string{"169.254.0.0/16"}
	}
	defer f.Close()

	return parseLocalBypassV4(bufio.NewScanner(f), tunIface)
}

func parseLocalBypassV4(sc *bufio.Scanner, tunIface string) []string {
	seen := map[string]bool{"169.254.0.0/16": true}
	out := []string{"169.254.0.0/16"}
	add := func(p netip.Prefix) {
		s := p.Masked().String()
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}

	first := true
	for sc.Scan() {
		if first { // header line
			first = false
			continue
		}

		fl := strings.Fields(sc.Text())
		if len(fl) < 8 || tunnelLike(fl[0], tunIface) {
			continue
		}

		dst, okD := procAddr(fl[1])
		gw, okG := procAddr(fl[2])
		mask, okM := procAddr(fl[7])
		if !okD || !okG || !okM {
			continue
		}

		bits := 0
		for _, b := range mask.As4() {
			for ; b != 0; b <<= 1 {
				bits++
			}
		}

		if bits == 0 { // the default route: keep only its gateway
			if gw.IsUnspecified() {
				continue
			}
			add(netip.PrefixFrom(gw, 32))

			continue
		}

		add(netip.PrefixFrom(dst, bits))
	}

	return out
}

// procAddr reads /proc/net/route's little-endian hex IPv4.
func procAddr(h string) (netip.Addr, bool) {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 4 {
		return netip.Addr{}, false
	}

	var a [4]byte
	byteorder.LittleEndian.PutUint32(a[:], byteorder.BigEndian.Uint32(b))

	return netip.AddrFrom4(a), true
}

func tunnelLike(dev, tunIface string) bool {
	if dev == "lo" || (tunIface != "" && dev == tunIface) {
		return true
	}

	for _, p := range []string{"dvpn-", "wg", "awg", "tun", "tap", "utun"} {
		if strings.HasPrefix(dev, p) {
			return true
		}
	}

	return false
}
