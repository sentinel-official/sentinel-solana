package wgcore

import (
	"net/netip"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// flushConntrack: a removed peer's NAT flows go before its address is reused.
func flushConntrack(addrs []netip.Addr) (uint, error) { return service.FlushConntrack(addrs) }
