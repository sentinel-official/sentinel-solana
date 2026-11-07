//go:build linux

package provider_test

import (
	"errors"
	"testing"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/provider"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// A plan's connection limit must be reachable: with a 5 GB plan, 6 connections and 1 GB tickets,
// every one of the 6 connections gets a ticket, and their caps fit in the plan.
func TestRuntime_ReserveTicket_EveryConnectionOfAPlanGetsATicket(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	home.Loaded.Provider.Tickets.DataCapBytes = session.BytesPerGB
	r := newRuntime(t, home)
	now := time.Now()
	sub := dvpn.Subscription{DataCapGB: 5, ExpiresAt: now.Unix() + 100_000, MaxConnections: 6, OpenedAt: now.Unix() - 100}
	request := auth.SignedTicketRequest{Request: auth.TicketRequest{Subscription: solana.PublicKey{3}}}

	var total uint64

	for i := range 6 {
		ticket, err := provider.ReserveTicket(t.Context(), r, request, sub, auth.Nonce{byte(i + 1)}, now, now.Unix()+60)
		require.NoError(t, err, "connection %d of 6", i+1)

		total += ticket.DataCapBytes
	}

	require.LessOrEqual(t, total, 5*session.BytesPerGB)

	_, err := provider.ReserveTicket(t.Context(), r, request, sub, auth.Nonce{7}, now, now.Unix()+60)
	require.ErrorIs(t, err, api.ErrPeerLimitReached, "a 7th connection is over the plan's limit")
}

func TestRuntime_IssueTicket_CapsDataCap(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	home.Loaded.Provider.Tickets.DataCapBytes = 500_000_000
	r := newRuntime(t, home)
	userKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	planAddress, subAddress, nodeAddress := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	now := time.Now().Unix()
	seedSubscriptionAccount(t, home, subAddress, dvpn.Subscription{
		DataCapGB: 10, ExpiresAt: now + 100_000, MaxConnections: 2, OpenedAt: now - 100,
		Owner: userKey.PublicKey(), Plan: planAddress, Provider: providerAddress(t, home),
	})
	require.NoError(t, home.DB.WithContext(t.Context()).Create(&provider.RosterNode{
		Node: nodeAddress.String(), Plan: planAddress.String(),
	}).Error)
	require.NoError(t, home.DB.WithContext(t.Context()).Create(&provider.Lease{
		Address: solana.PublicKey{4}.String(), Healthy: true, Node: nodeAddress.String(),
	}).Error)

	encoded := encodeTicketRequest(t, userKey, providerAddress(t, home), subAddress, nodeAddress, now)
	result, err := provider.IssueTicket(t.Context(), r, encoded)

	require.NoError(t, err)
	require.Equal(t, uint64(500_000_000), result.DataCapBytes)
}

func TestRuntime_IssueTicket_RejectsStaleRequest(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	userKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	planAddress, subAddress, nodeAddress := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	now := time.Now().Unix()
	seedSubscriptionAccount(t, home, subAddress, dvpn.Subscription{
		DataCapGB: 10, ExpiresAt: now + 100_000, MaxConnections: 2, OpenedAt: now - 100,
		Owner: userKey.PublicKey(), Plan: planAddress, Provider: providerAddress(t, home),
	})

	stale := encodeTicketRequest(t, userKey, providerAddress(t, home), subAddress, nodeAddress, now-600)
	_, err = provider.IssueTicket(t.Context(), r, stale)

	require.ErrorIs(t, err, auth.ErrTicketRequestStale)
}

// A cancelled and re-bought subscription has the same address. Tickets issued before the
// new opening must not count against its connection limit; tickets of this opening still do.
func TestRuntime_ReserveTicket_RebuyIgnoresOldOpeningTickets(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		issued  int64 // relative to the new opening
		wantErr error
	}{
		{name: "ticket from the previous opening", issued: -50},
		{name: "ticket from this opening", issued: 10, wantErr: api.ErrPeerLimitReached},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
			r := newRuntime(t, home)
			now := time.Now()
			subAddress := solana.PublicKey{2}
			opened := now.Unix() - 100

			require.NoError(t, home.DB.WithContext(t.Context()).Create(&provider.Ticket{
				ExpiresUnix:  now.Unix() + 3600,
				Hash:         "old-ticket",
				IssuedUnix:   opened + tc.issued,
				Subscription: subAddress.String(),
			}).Error)

			_, err := provider.ReserveTicket(t.Context(), r, auth.SignedTicketRequest{
				Request: auth.TicketRequest{Subscription: subAddress},
			}, dvpn.Subscription{ExpiresAt: now.Unix() + 100_000, MaxConnections: 1, OpenedAt: opened},
				auth.Nonce{}, now, now.Unix()+60)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRuntime_ReserveTicket_SubscriptionReadFailure(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	wantErr := errors.New("subscription read failed")

	require.NoError(t, home.DB.Callback().Query().Before("gorm:query").Register(
		"test:subscription_failure",
		func(db *gorm.DB) {
			if db.Statement.Table == "subscriptions" {
				db.Error = wantErr
			}
		},
	))

	now := time.Now()
	_, err := provider.ReserveTicket(t.Context(), r, auth.SignedTicketRequest{
		Request: auth.TicketRequest{Subscription: solana.PublicKey{1}},
	}, dvpn.Subscription{DataCapGB: 1}, auth.Nonce{}, now, now.Unix()+60)

	require.ErrorIs(t, err, wantErr)

	var count int64
	require.NoError(t, home.DB.WithContext(t.Context()).Model(&provider.Ticket{}).Count(&count).Error)
	require.Zero(t, count)
}
