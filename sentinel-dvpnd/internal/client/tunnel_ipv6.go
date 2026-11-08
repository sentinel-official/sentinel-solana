package client

import (
	"net/netip"
	"slices"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
)

// tunnelIPv6Setter is implemented by service clients whose tunnel answers TCP on the client itself
// (hysteria2's system stack). Through such a tunnel an IPv6 connect to a node with no IPv6 succeeds
// locally and is reset after the first bytes, so applications never fall back to IPv4.
type tunnelIPv6Setter interface {
	SetTunnelIPv6(on bool)
}

// setTunnelIPv6 keeps IPv6 out of the tunnel when the node publishes no IPv6 address and the kill
// switch is on. The kill switch then refuses IPv6 on the host at once, so applications fall back to
// IPv4 and nothing leaks. With the kill switch off IPv6 stays in the tunnel, as before.
func setTunnelIPv6(svc any, killSwitch string, nodeAddrs []netip.Addr) {
	setter, ok := svc.(tunnelIPv6Setter)
	if !ok {
		return
	}

	setter.SetTunnelIPv6(killSwitch == config.KillSwitchOff || slices.ContainsFunc(nodeAddrs, isIPv6))
}

func isIPv6(a netip.Addr) bool {
	return a.Is6() && !a.Is4In6()
}
