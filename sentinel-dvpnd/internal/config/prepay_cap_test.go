package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
)

// Voucher mode never refunds the unused part of the last prepay block, so the client's cap is the most a session
// can lose. With the defaults, a node that demands a 2 GB (or 4 h) block is refused; a node on the node defaults
// is not.
func TestDefaultClient_PrepayCapBoundsTheBlock(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultClient()
	node := config.DefaultNode().Sessions

	cases := []struct {
		basis      session.Basis
		configured uint64
		err        error
		maximum    uint64
		minimum    uint64
		name       string
		want       uint64
	}{
		{
			basis: session.BasisPerGB, configured: cfg.PrepayBytes, err: session.ErrPrepayExceedsMaximum,
			maximum: cfg.MaxPrepayBytes, minimum: 2_000_000_000, name: "2 GB node minimum",
		},
		{
			basis: session.BasisPerHour, configured: cfg.PrepaySeconds, err: session.ErrPrepayExceedsMaximum,
			maximum: cfg.MaxPrepaySeconds, minimum: 14_400, name: "4 h node minimum",
		},
		{
			basis: session.BasisPerGB, configured: cfg.PrepayBytes,
			maximum: cfg.MaxPrepayBytes, minimum: node.MinPrepayBytes, name: "node default bytes", want: cfg.PrepayBytes,
		},
		{
			basis: session.BasisPerHour, configured: cfg.PrepaySeconds,
			maximum: cfg.MaxPrepaySeconds, minimum: node.MinPrepaySeconds, name: "node default seconds", want: cfg.PrepaySeconds,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			terms := session.Terms{Basis: tc.basis, Price: 1_000}

			block, err := session.PrepayBlock(terms, tc.configured, tc.minimum, tc.maximum)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, block)
		})
	}
}
