package wgcore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fitWith runs fitMTU's logic with a fixed uplink (tests are parallel, so no global swap). padded and
// the uplinkMTU stub come from the per-OS test files: mtu_linux_test.go on Linux, mtu_other_test.go
// elsewhere.
func fitWith(node uint16, obf any, up func(string) int) uint16 {
	overhead := wgOverhead
	if o, ok := obf.(overheader); ok {
		overhead += o.TransportOverhead()
	}

	return fitMTUWith(node, overhead, up("203.0.113.1"))
}

func TestFitMTU(t *testing.T) {
	t.Parallel()

	up := func(n int) func(string) int { return func(string) int { return n } }
	cases := []struct {
		name   string
		uplink int
		node   uint16
		obf    any
		want   uint16
	}{
		{"wireguard on a 1500 uplink keeps 1420", 1500, 1420, struct{}{}, 1420},
		{"wireguard on a 1460 uplink", 1460, 1420, struct{}{}, 1380},
		{"amneziawg S4=32 on a 1500 uplink", 1500, 1420, padded{32}, 1388},
		{"amneziawg S4=32 on a 1460 uplink", 1460, 1420, padded{32}, 1348},
		{"never raises the node MTU", 9000, 1420, padded{8}, 1420},
		{"floor 1280", 1300, 1420, padded{32}, 1280},
		{"unknown uplink keeps the node MTU", 0, 1420, padded{32}, 1420},
		{"node MTU unset stays unset", 1500, 0, padded{32}, 0},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, fitWith(tc.node, tc.obf, up(tc.uplink)), tc.name)
	}
}
