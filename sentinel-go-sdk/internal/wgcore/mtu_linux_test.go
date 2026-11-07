package wgcore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests never look up the real uplink: the configured MTU is used unchanged unless a test sets one.
func init() { uplinkMTU = func(string) int { return 0 } }

type padded struct{ s4 int }

func (p padded) TransportOverhead() int { return p.s4 }

func TestServerMTU_FitsTheNodeUplink(t *testing.T) {
	t.Parallel()

	var asked []string

	up := func(n int) func(string) int {
		return func(host string) int { asked = append(asked, host); return n }
	}

	cases := []struct {
		name   string
		uplink int
		cfg    uint16
		ext    any
		want   uint16
	}{
		{"wireguard on a 1500 uplink keeps 1420", 1500, 1420, struct{}{}, 1420},
		{"wireguard on a 1460 uplink", 1460, 1420, struct{}{}, 1380},
		{"amneziawg S4=32 on a 1500 uplink", 1500, 1420, padded{32}, 1388},
		{"amneziawg S4=32 on a 1460 uplink", 1460, 1420, padded{32}, 1348},
		{"never raises the configured MTU", 9000, 1420, padded{8}, 1420},
		{"floor 1280", 1300, 1420, padded{32}, 1280},
		{"unknown uplink keeps the configured MTU", 0, 1420, padded{32}, 1420},
		{"unset MTU stays unset", 1500, 0, padded{32}, 0},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, serverMTUWith(tc.cfg, tc.ext, up(tc.uplink)), tc.name)
	}

	for _, host := range asked {
		require.Equal(t, serverRouteHost, host)
	}
}

func TestServerMTU_UnknownUplinkInTests(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint16(1420), serverMTU(uint16(1420), padded{32}))
}
