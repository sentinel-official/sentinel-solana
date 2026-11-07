package hysteria2

import (
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"net/netip"
)

const Binary = binary

func WriteConf(path string, addr netip.Addr, iface string, resp PeerResponse) error {
	return writeConf(path, addr, iface, resp)
}

type TunMeter = tunMeter

func (m *TunMeter) Add(raw service.PeerStats) service.PeerStats { return m.add(raw) }
