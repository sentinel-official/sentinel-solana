package wgcore

import (
	"net"
	"net/netip"
)

// A WireGuard transport packet adds up to 80 bytes over IPv6 (40 IP + 8 UDP + 32 WireGuard), and
// obfuscation can pad every transport packet on top (AmneziaWG S4). If the tunnel MTU is above
// uplink MTU - overhead, full-size packets from the node fragment or are dropped: on a node with a
// 1460-byte NIC (e.g. GCP) advertising 1420, TLS through the tunnel hangs after the handshake while
// small packets pass.
const wgOverhead = 80

// serverRouteHost is a public address used only to pick the node's default route (no packet is sent).
const serverRouteHost = "1.1.1.1"

// overheader is implemented by extensions that add bytes to every transport packet.
type overheader interface {
	TransportOverhead() int
}

// uplinkMTU returns the MTU of the interface that routes to host, or 0 when unknown. A UDP
// "dial" only picks a route: it sends no packet and changes nothing on the machine.
var uplinkMTU = func(host string) int {
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return 0
	}

	conn, err := net.Dial("udp", netip.AddrPortFrom(addr, 9).String())
	if err != nil {
		return 0
	}

	local, _ := conn.LocalAddr().(*net.UDPAddr)
	_ = conn.Close()

	if local == nil {
		return 0
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return 0
	}

	for _, ifc := range ifaces {
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}

		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.Equal(local.IP) {
				return ifc.MTU
			}
		}
	}

	return 0
}

// serverMTU is the MTU the node uses for its interface and advertises to peers: the configured MTU
// fitted to the node's own default-route uplink. It never raises the configured value.
func serverMTU[E any](cfgMTU uint16, extensions E) uint16 {
	return serverMTUWith(cfgMTU, extensions, uplinkMTU)
}

func serverMTUWith[E any](cfgMTU uint16, extensions E, up func(host string) int) uint16 {
	overhead := wgOverhead
	if o, ok := any(extensions).(overheader); ok {
		overhead += o.TransportOverhead()
	}

	return fitMTUWith(cfgMTU, overhead, up(serverRouteHost))
}

func fitMTUWith(mtu uint16, overhead, up int) uint16 {
	if up <= 0 || mtu == 0 {
		return mtu
	}

	limit := up - overhead
	if limit < minMTU {
		limit = minMTU
	}

	if int(mtu) > limit {
		return uint16(limit) //nolint:gosec // bounded by the uplink MTU
	}

	return mtu
}
