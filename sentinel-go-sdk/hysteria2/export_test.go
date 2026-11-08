package hysteria2

import (
	"net/netip"
)

const Binary = binary

func WriteConf(path string, addr netip.Addr, iface string, resp PeerResponse) error {
	return writeConf(path, addr, iface, resp, false)
}

func WriteConfNoIPv6(path string, addr netip.Addr, iface string, resp PeerResponse) error {
	return writeConf(path, addr, iface, resp, true)
}
