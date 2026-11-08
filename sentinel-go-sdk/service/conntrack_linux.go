package service

import (
	"errors"
	"fmt"
	"net"
	"net/netip"

	"github.com/vishvananda/netlink"
)

// FlushConntrack deletes the kernel's connection-tracking entries to or from a removed peer's
// tunnel addresses. NAT flows the old peer left open (ESTABLISHED, timeouts of days) stay
// in the table after the peer is gone; when the address went to the next peer, the replies to the
// old peer's connections were delivered to, and billed against, the new one. Returns how many
// entries were deleted.
func FlushConntrack(addrs []netip.Addr) (uint, error) {
	var (
		deleted uint
		errs    []error
	)

	for _, addr := range addrs {
		family := netlink.InetFamily(netlink.FAMILY_V4)
		if addr.Is6() {
			family = netlink.InetFamily(netlink.FAMILY_V6)
		}

		ip := net.IP(addr.AsSlice())

		src := &netlink.ConntrackFilter{}
		if err := src.AddIP(netlink.ConntrackOrigSrcIP, ip); err != nil {
			errs = append(errs, fmt.Errorf("conntrack filter src %s: %w", addr, err))

			continue
		}

		dst := &netlink.ConntrackFilter{}
		if err := dst.AddIP(netlink.ConntrackOrigDstIP, ip); err != nil {
			errs = append(errs, fmt.Errorf("conntrack filter dst %s: %w", addr, err))

			continue
		}

		n, err := netlink.ConntrackDeleteFilters(netlink.ConntrackTable, family, src, dst)
		deleted += n

		if err != nil {
			errs = append(errs, fmt.Errorf("deleting conntrack entries of %s: %w", addr, err))
		}
	}

	return deleted, errors.Join(errs...)
}
