package hysteria2_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/hysteria2"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// Lab counts: a TCP download crosses the tun on both legs, a UDP upload once.
// The meter must count each once, also when the UDP upload follows a TCP
// download in the same session (max over running totals would miss it).
func TestTunMeter(t *testing.T) {
	t.Parallel()

	var m hysteria2.TunMeter

	total := func(rx, tx uint64) uint64 {
		got := m.Add(service.PeerStats{RxBytes: rx, TxBytes: tx})
		require.Zero(t, got.TxBytes)

		return got.RxBytes
	}

	require.Zero(t, total(0, 0))
	require.Equal(t, uint64(263_000_000), total(263_000_000, 254_000_000)) // tcp download
	require.Equal(t, uint64(270_191_124), total(263_000_000, 261_191_124)) // then udp upload 7.19 MB
	require.Equal(t, uint64(275_191_124), total(268_000_000, 261_191_124)) // then udp download 5 MB
	require.Equal(t, uint64(275_191_124), total(268_000_000, 261_191_124)) // idle
	require.Equal(t, uint64(275_191_224), total(100, 0))                   // counters restarted
}

// Headers come off per packet in the counted direction, never below zero.
func TestTunMeter_Headers(t *testing.T) {
	t.Parallel()

	var m hysteria2.TunMeter

	add := func(rx, rxPkts, tx, txPkts uint64) uint64 {
		return m.Add(service.PeerStats{RxBytes: rx, RxPackets: rxPkts, TxBytes: tx, TxPackets: txPkts}).RxBytes
	}

	// 1,000 full-size packets down (1,400 B each), 1,000 small ACKs up.
	require.Equal(t, uint64(1_400_000-1_000*28), add(1_400_000, 1_000, 52_000, 1_000))
	// Upload: tx is larger, so tx packets are deducted.
	require.Equal(t, uint64(1_372_000+700_000-500*28), add(1_400_000, 1_000, 752_000, 1_500))
	// Tiny packets cannot go negative.
	before := add(1_400_000, 1_000, 752_000, 1_500)
	require.Equal(t, before, add(1_400_010, 2_000, 752_000, 1_500))
}
