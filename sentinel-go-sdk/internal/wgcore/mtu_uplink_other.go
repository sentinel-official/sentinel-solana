//go:build !linux

package wgcore

import (
	"net"
	"net/netip"
)

// The client fits its MTU on every OS (mtu.go). On Linux these helpers come from mtu_linux.go (the
// node's MTU fit); this is the same code for the other client OSes, kept separate so mtu_linux.go
// stays as upstream has it.

const wgOverhead = 80

// overheader is implemented by extensions that add bytes to every transport packet.
type overheader interface {
	TransportOverhead() int
}

// uplinkMTU returns the MTU of the interface that routes to host, or 0 when unknown. A UDP
// "dial" only picks a route: it sends no packet and changes nothing on the machine.
//
//nolint:gochecknoglobals // tests replace it
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
