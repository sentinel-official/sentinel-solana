package client_test

import (
	"math"
	"testing"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-go-sdk/v3/voucher"
)

func TestRuntime_BlockFor(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		basis      session.Basis
		configured uint64
		err        error
		minimum    uint64
		name       string
		want       uint64
	}{
		{basis: session.BasisPerGB, minimum: 100_000_000, name: "defaults use node minimum", want: 100_000_000},
		{
			basis: session.BasisPerGB, configured: 200_000_000, minimum: 100_000_000,
			name: "configured above minimum", want: 200_000_000,
		},
		{
			basis: session.BasisPerGB, configured: 1, minimum: 100_000_000,
			name: "configured below minimum", want: 100_000_000,
		},
		{
			basis: session.BasisPerGB, configured: 5_000_000_000, minimum: 100_000_000,
			name: "configured limited", want: config.DefaultClient().MaxPrepayBytes,
		},
		{basis: session.BasisPerGB, minimum: 1, name: "one-byte node minimum", want: 200_001},
		{
			basis: session.BasisPerGB, configured: config.DefaultClient().PrepayBytes, minimum: 1,
			name: "one-byte node minimum with the client default", want: 100_000_000,
		},
		{
			basis: session.BasisPerGB, err: session.ErrPrepayExceedsMaximum, minimum: 3_000_000_000,
			name: "node minimum above maximum",
		},
		{
			basis: session.BasisPerGB, err: session.ErrPrepayExceedsMaximum, minimum: 2_000_000_000,
			name: "2 GB node minimum is over the default cap",
		},
		{basis: session.BasisPerHour, minimum: 60, name: "hourly minimum", want: 60},
		{
			basis: session.BasisPerHour, configured: 20_000, minimum: 60,
			name: "hourly maximum", want: config.DefaultClient().MaxPrepaySeconds,
		},
		{
			basis: session.BasisPerHour, err: session.ErrPrepayExceedsMaximum, minimum: 14_400,
			name: "4 h node minimum is over the default cap",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			home := testutil.NewClientHome(t, sdktest.NewFakeRPC(t))
			home.Loaded.Client.PrepayBytes = tc.configured
			home.Loaded.Client.PrepaySeconds = tc.configured
			r := newRuntime(t, home, home.ProgramClient)
			settlement := nodeapi.Settlement{MinChannelPrepayBytes: tc.minimum, MinChannelPrepaySeconds: tc.minimum}

			block, err := client.BlockFor(r, session.Terms{Basis: tc.basis, Price: 5_000}, settlement)

			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.want, block)
		})
	}
}

func TestRuntime_BlockFor_NamesTheCap(t *testing.T) {
	t.Parallel()

	home := testutil.NewClientHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home, home.ProgramClient)
	settlement := nodeapi.Settlement{MinChannelPrepayBytes: 2_000_000_000, MinChannelPrepaySeconds: 14_400}

	_, err := client.BlockFor(r, session.Terms{Basis: session.BasisPerGB, Price: 5_000}, settlement)
	require.ErrorContains(t, err, "node minimum 2000000000 is over client max_prepay_bytes 268435456")

	_, err = client.BlockFor(r, session.Terms{Basis: session.BasisPerHour, Price: 5_000}, settlement)
	require.ErrorContains(t, err, "node minimum 14400 is over client max_prepay_seconds 1800")
}

func TestRuntime_BlockFor_RejectsZeroBlock(t *testing.T) {
	t.Parallel()

	home := testutil.NewClientHome(t, sdktest.NewFakeRPC(t))
	home.Loaded.Client.PrepayBytes = 0
	r := newRuntime(t, home, home.ProgramClient)
	_, err := client.BlockFor(r, session.Terms{Basis: session.BasisPerGB, Price: 0}, nodeapi.Settlement{})

	require.ErrorIs(t, err, session.ErrPrepayZero)
}

func TestRuntime_SettledBound(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		blockBaseUnits uint64
		name           string
		percent        uint8
		sessions       []client.Session
		want           uint64
	}{
		{
			blockBaseUnits: 500,
			name:           "ended sessions on this channel",
			percent:        5,
			sessions: []client.Session{
				{ChannelAddress: "channel", Clean: true, ID: "clean", OwedBaseUnits: 1_000, Status: client.SessionStatusEnded},
				{
					ChannelAddress: "channel", ID: "unclean", PaidBaseUnits: 700,
					SettledBaseUnits: 200, Status: client.SessionStatusEnded,
				},
				{
					ChannelAddress: "other-channel", Clean: true, ID: "other",
					OwedBaseUnits: 1_000, Status: client.SessionStatusEnded,
				},
				{
					ChannelAddress: "channel", Clean: true, ID: "active",
					OwedBaseUnits: 1_000, Status: client.SessionStatusActive,
				},
			},
			want: 1_575,
		},
		{
			blockBaseUnits: 1,
			name:           "round each clean term up",
			percent:        5,
			sessions: []client.Session{
				{ChannelAddress: "channel", Clean: true, ID: "clean", OwedBaseUnits: 1, Status: client.SessionStatusEnded},
			},
			want: 3,
		},
		{
			name: "settled above paid contributes zero",
			sessions: []client.Session{
				{
					ChannelAddress: "channel", ID: "unclean", PaidBaseUnits: 1,
					SettledBaseUnits: 2, Status: client.SessionStatusEnded,
				},
			},
		},
		{
			blockBaseUnits: 500,
			name:           "clean bound saturates",
			percent:        100,
			sessions: []client.Session{
				{
					ChannelAddress: "channel", Clean: true, ID: "clean",
					OwedBaseUnits: math.MaxInt64, Status: client.SessionStatusEnded,
				},
			},
			want: math.MaxUint64,
		},
		{
			name: "sum saturates",
			sessions: []client.Session{
				{ChannelAddress: "channel", ID: "first", PaidBaseUnits: math.MaxInt64, Status: client.SessionStatusEnded},
				{ChannelAddress: "channel", ID: "second", PaidBaseUnits: math.MaxInt64, Status: client.SessionStatusEnded},
				{ChannelAddress: "channel", ID: "third", PaidBaseUnits: 2, Status: client.SessionStatusEnded},
			},
			want: math.MaxUint64,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			home := testutil.NewClientHome(t, sdktest.NewFakeRPC(t))
			r := newRuntime(t, home, home.ProgramClient)

			if len(tc.sessions) > 0 {
				require.NoError(t, home.DB.WithContext(t.Context()).Create(&tc.sessions).Error)
			}

			bound, err := client.SettledBound(t.Context(), r, "channel", tc.blockBaseUnits, tc.percent)

			require.NoError(t, err)
			require.Equal(t, tc.want, bound)
		})
	}
}

func TestRuntime_VerifyLedger(t *testing.T) {
	t.Parallel()

	home := testutil.NewClientHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home, home.ProgramClient)
	address := home.NodeAddress
	channel := &client.Channel{Address: address.String(), Escrow: 1_000_000_000, OpenedUnix: 7, Redeemed: 100}
	terms := session.Terms{Basis: session.BasisPerGB, Price: 5_000}
	signed := voucher.Sign(home.DaemonKey, voucher.ChannelMessage(address, 7, 200))
	otherKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	otherSigned := voucher.Sign(otherKey, voucher.ChannelMessage(address, 7, 200))

	for _, tc := range []struct {
		change func(*nodeapi.Position)
		err    error
		name   string
	}{
		{name: "signed by this device passes"},
		{
			change: func(view *nodeapi.Position) { view.Ledger.Signature = nil },
			err:    session.ErrInvalidLedger,
			name:   "unsigned above redeemed fails",
		},
		{
			change: func(view *nodeapi.Position) { view.Ledger.Signature = otherSigned.Signature[:] },
			err:    session.ErrInvalidLedger,
			name:   "signed by another device fails",
		},
		{
			change: func(view *nodeapi.Position) {
				view.Ledger.CumulativeBaseUnits = 100
				view.Ledger.Signature = nil
			},
			name: "at redeemed needs no signature",
		},
		{
			change: func(view *nodeapi.Position) { view.Ledger.EscrowBaseUnits++ },
			err:    session.ErrLedgerMismatch,
			name:   "escrow mismatch",
		},
		{
			change: func(view *nodeapi.Position) { view.Ledger.RedeemedBaseUnits++ },
			err:    session.ErrLedgerMismatch,
			name:   "redeemed mismatch",
		},
		{
			change: func(view *nodeapi.Position) { view.Ledger.OpenedAt++ },
			err:    session.ErrLedgerMismatch,
			name:   "opened time mismatch",
		},
		{
			change: func(view *nodeapi.Position) { view.Consumption.OwedBaseUnits = 201 },
			err:    session.ErrOwedExceedsPaid,
			name:   "owed above paid",
		},
		{
			change: func(view *nodeapi.Position) { view.Consumption.SettledBaseUnits = 51 },
			err:    session.ErrSettledExceedsBound,
			name:   "settled above bound",
		},
		{
			change: func(view *nodeapi.Position) { view.Ledger.Price++ },
			err:    session.ErrNodePriceChanged,
			name:   "repriced same basis",
		},
		{
			change: func(view *nodeapi.Position) {
				view.Ledger.Basis = session.BasisPerHour
				view.Ledger.Price++
			},
			name: "other basis price ignored",
		},
		{
			change: func(view *nodeapi.Position) {
				view.Ledger.EscrowBaseUnits++
				view.Ledger.Status = nodeapi.LedgerStatusClosing
			},
			err:  api.ErrChannelNotOpen,
			name: "closing checked before chain mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			view := nodeapi.Position{
				Consumption: nodeapi.Consumption{OwedBaseUnits: 100, SettledBaseUnits: 50},
				Ledger: nodeapi.Ledger{
					Basis:               session.BasisPerGB,
					CumulativeBaseUnits: 200,
					EscrowBaseUnits:     1_000_000_000,
					OpenedAt:            7,
					Price:               5_000,
					RedeemedBaseUnits:   100,
					Signature:           signed.Signature[:],
					Status:              nodeapi.LedgerStatusOpen,
				},
			}
			if tc.change != nil {
				tc.change(&view)
			}

			err := client.VerifyLedger(r, channel, view, terms, 50)

			require.ErrorIs(t, err, tc.err)
		})
	}
}
