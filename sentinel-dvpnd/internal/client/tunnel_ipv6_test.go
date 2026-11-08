package client

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
)

type tunnelIPv6Recorder struct {
	calls []bool
}

func (r *tunnelIPv6Recorder) SetTunnelIPv6(on bool) {
	r.calls = append(r.calls, on)
}

func TestSetTunnelIPv6(t *testing.T) {
	t.Parallel()

	v4 := []netip.Addr{netip.MustParseAddr("203.0.113.1")}
	mapped := []netip.Addr{netip.MustParseAddr("::ffff:203.0.113.1")}
	dual := []netip.Addr{netip.MustParseAddr("203.0.113.1"), netip.MustParseAddr("2001:db8::1")}

	cases := []struct {
		name       string
		killSwitch string
		addrs      []netip.Addr
		want       bool
	}{
		{"IPv4-only node, kill switch on", config.KillSwitchOn, v4, false},
		{"IPv4-only node, kill switch persist", config.KillSwitchPersist, v4, false},
		{"IPv4-mapped address is not IPv6", config.KillSwitchOn, mapped, false},
		{"node with an IPv6 address", config.KillSwitchOn, dual, true},
		{"kill switch off keeps IPv6 in the tunnel", config.KillSwitchOff, v4, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &tunnelIPv6Recorder{}
			setTunnelIPv6(rec, tc.killSwitch, tc.addrs)
			require.Equal(t, []bool{tc.want}, rec.calls)
		})
	}

	require.NotPanics(t, func() { setTunnelIPv6(struct{}{}, config.KillSwitchOn, v4) }, "services without the setter are left alone")
}
