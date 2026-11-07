//go:build linux

package provider

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Every connection the plan allows must be able to get a ticket.
func TestFairTicketCap_EveryConnectionFits(t *testing.T) {
	t.Parallel()

	const gb = uint64(1_000_000_000)

	// 5 GB plan, 6 connections, 1 GB configured: each ticket gets 5/6 GB, so six fit.
	share := fairTicketCap(5*gb, 6, gb)
	require.Equal(t, 5*gb/6, share)

	allocated := uint64(0)
	for i := range 6 {
		c := ticketDataCap(5*gb, allocated, share)
		require.NotZero(t, c, "connection %d", i+1)
		allocated += c
	}

	// The configured cap still wins when it is smaller than the share.
	require.Equal(t, uint64(500_000_000), fairTicketCap(10*gb, 2, 500_000_000))
	// No configured cap: the share.
	require.Equal(t, 5*gb, fairTicketCap(10*gb, 2, 0))
	// Unlimited data or no connection limit: unchanged.
	require.Equal(t, gb, fairTicketCap(0, 6, gb))
	require.Equal(t, gb, fairTicketCap(5*gb, 0, gb))
}
