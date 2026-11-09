//go:build linux

package provider_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/provider"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

func TestLeaseActions(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultProvider()
	interval := int64(provider.ProbeNodesInterval.Seconds())
	now := time.Now().Unix()

	cases := []struct {
		deadSinceUnix int64
		lease         *provider.Lease
		live          bool
		name          string

		ratePerHour uint64
		want        []provider.Action
	}{
		{

			live: true, name: "no lease live node opens",
			ratePerHour: 1_000,

			want: []provider.Action{provider.NewAction(provider.ActionOpen, 1_000*cfg.Lease.ReserveHours)},
		},
		{

			live: false, name: "no lease dead node does nothing",
			ratePerHour: 1_000,

			want: nil,
		},
		{

			lease: &provider.Lease{
				Status: provider.StatusClosing,
			},

			live: true, name: "closing lease does nothing",

			ratePerHour: 1_000,

			want: nil,
		},
		{
			lease: &provider.Lease{
				Escrow: 200_000, Mode: dvpn.PositionModeVoucher,
				OpenedUnix: now - int64(200_000-3_600*cfg.Lease.TopupHours+1), Status: provider.StatusOpen,
			},
			live: true, name: "voucher lease below topup funds", ratePerHour: 3_600,
			want: []provider.Action{provider.NewAction(provider.ActionFund, 3_600*cfg.Lease.ReserveHours)},
		},
		{
			lease: &provider.Lease{
				Escrow: 200_000, Mode: dvpn.PositionModeVoucher,
				OpenedUnix: now - int64(200_000-3_600*cfg.Lease.TopupHours), Status: provider.StatusOpen,
			},
			live: true, name: "voucher lease at topup does nothing", ratePerHour: 3_600,
		},
		{

			lease: &provider.Lease{
				Escrow:            500,
				Mode:              dvpn.PositionModeStream,
				RatePerHour:       1_000,
				Status:            provider.StatusOpen,
				StreamBase:        0,
				StreamStartedUnix: now,
			},

			live: true, name: "stream lease below topup funds",

			ratePerHour: 1_000,

			want: []provider.Action{provider.NewAction(provider.ActionFund, 1_000*cfg.Lease.ReserveHours)},
		},
		{

			deadSinceUnix: now - 3*interval - 1, lease: &provider.Lease{
				Status: provider.StatusOpen,
			},

			live: false, name: "dead since three intervals closes",

			ratePerHour: 1_000,

			want: []provider.Action{provider.NewAction(provider.ActionStartClose, 0)},
		},
		{

			deadSinceUnix: now - interval, lease: &provider.Lease{
				Status: provider.StatusOpen,
			},

			live: false, name: "dead since within grace does nothing",

			ratePerHour: 1_000,

			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := provider.LeaseActions(tc.lease, tc.live, tc.deadSinceUnix, tc.ratePerHour, &cfg, now)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRuntime_CloseStreamLease_DeduplicatesConcurrentRequests(t *testing.T) {
	t.Parallel()
	home, r, lease, _ := newLeaseWatchFixture(t)
	seedStreamLease(t, home, lease, solana.PublicKey{2})

	results := make(chan error, 2)

	for range 2 {
		go func() {
			closing := lease
			results <- provider.CloseStreamLease(t.Context(), r, &closing)
		}()
	}

	for range 2 {
		require.NoError(t, <-results)
	}

	require.Len(t, home.RPC.Sent(), 1)

	var stored provider.Lease
	require.NoError(t, home.DB.First(&stored, "address = ?", lease.Address).Error)
	require.Equal(t, provider.StatusClosed, stored.Status)
}

func TestRuntime_FinalizeClosing_SendsCloseLeaseAfterGrace(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))

	const grace = uint32(3_600)
	require.NoError(t, home.RPC.SetConfig(home.ProgramClient.ProgramID, dvpn.Config{
		LeaseCloseGraceSeconds: grace, Mint: home.ProgramClient.Mint,
	}))
	r := newRuntime(t, home)
	require.NoError(t, r.Start(t.Context()))

	nodeAddress, opener := solana.PublicKey{3}, solana.PublicKey{4}
	elapsedAddress, withinGraceAddress, delegatedAddress := solana.PublicKey{5}, solana.PublicKey{6}, solana.PublicKey{7}
	now := time.Now().Unix()
	elapsedLease := dvpn.Lease{
		CloseStartedAt: now - int64(grace) - 10, Node: nodeAddress, Opener: home.OwnerAddress, Owner: home.OwnerAddress,
		Provider: providerAddress(t, home), RentRecipient: home.DaemonKey.PublicKey(), Status: dvpn.PositionStatusClosing,
	}
	home.RPC.SetAccount(elapsedAddress, sdktest.Account{
		Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, elapsedLease), Owner: home.ProgramClient.ProgramID,
	})
	withinGraceLease := elapsedLease
	withinGraceLease.CloseStartedAt = now - 10
	home.RPC.SetAccount(withinGraceAddress, sdktest.Account{
		Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, withinGraceLease), Owner: home.ProgramClient.ProgramID,
	})

	delegatedLease := elapsedLease
	delegatedLease.Opener = opener
	home.RPC.SetAccount(delegatedAddress, sdktest.Account{
		Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, delegatedLease), Owner: home.ProgramClient.ProgramID,
	})

	absentAddress := solana.PublicKey{8}
	for _, address := range []solana.PublicKey{elapsedAddress, withinGraceAddress, delegatedAddress, absentAddress} {
		require.NoError(t, home.DB.WithContext(t.Context()).Create(&provider.Lease{
			Address: address.String(), Status: provider.StatusClosing,
		}).Error)
	}

	grant, _, err := dvpn.FindGrantAddress(
		home.ProgramClient.ProgramID, home.OwnerAddress, home.DaemonKey.PublicKey(), solana.PublicKey{},
	)
	require.NoError(t, err)

	signature := []byte("accepted signature")

	require.NoError(t, home.DB.Callback().Query().After("gorm:query").Register("provider_test:accepted_voucher",
		func(db *gorm.DB) {
			if _, ok := db.Statement.Dest.(*[]provider.Lease); !ok {
				return
			}

			require.NoError(t, home.DB.Session(&gorm.Session{NewDB: true}).Model(&provider.Lease{}).
				Where("status = ?", provider.StatusClosing).Updates(map[string]any{
				"cumulative": uint64(90_000), "signature": signature,
			}).Error)
		}))
	require.NoError(t, provider.FinalizeClosing(t.Context(), r))

	var closed provider.Lease
	require.NoError(t, home.DB.WithContext(t.Context()).First(&closed, "address = ?", elapsedAddress.String()).Error)
	require.Equal(t, provider.StatusClosed, closed.Status)
	require.False(t, closed.Healthy)

	for _, address := range []solana.PublicKey{elapsedAddress, delegatedAddress, absentAddress} {
		var stored provider.Lease
		require.NoError(t, home.DB.First(&stored, "address = ?", address.String()).Error)
		require.Equal(t, provider.StatusClosed, stored.Status)
		require.Equal(t, uint64(90_000), stored.Cumulative)
		require.Equal(t, signature, stored.Signature)
	}

	var untouched provider.Lease
	require.NoError(t, home.DB.WithContext(t.Context()).
		First(&untouched, "address = ?", withinGraceAddress.String()).Error)
	require.Equal(t, provider.StatusClosing, untouched.Status)

	sent := home.RPC.Sent()
	require.Len(t, sent, 2)

	grants := closeLeaseGrants(t, sent)
	require.Len(t, grants, 2)
	require.Equal(t, grant, grants[elapsedAddress])
	require.Equal(t, grant, grants[delegatedAddress])
}

func TestRuntime_KeepLeases_ClosesLeaseOfClosedNode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		account sdktest.Account
		name    string
	}{
		{name: "absent"},
		{
			account: sdktest.Account{Owner: solana.PublicKey{1}},
			name:    "owner mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			home, r, lease, probe := newLeaseWatchFixture(t)
			require.NoError(t, home.DB.Model(&provider.Lease{}).
				Where("address = ?", lease.Address).Update("node_opened_unix", lease.NodeOpenedUnix-1).Error)
			address := solana.MustPublicKeyFromBase58(lease.Address)
			home.RPC.SetAccount(address, sdktest.Account{
				Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, dvpn.Lease{
					Escrow:       lease.Escrow,
					Mode:         lease.Mode,
					Node:         home.NodeAddress,
					NodeOpenedAt: lease.NodeOpenedUnix,
					OpenedAt:     lease.OpenedUnix,
					Provider:     providerAddress(t, home),
					Status:       dvpn.PositionStatusOpen,
				}),
				Owner: home.ProgramClient.ProgramID,
			})
			require.NoError(t, home.DB.Create(&provider.RosterNode{
				Node: probe.Node,
				Plan: planName,
			}).Error)
			home.RPC.SetAccount(home.NodeAddress, tc.account)

			require.NoError(t, provider.KeepLeases(t.Context(), r))

			var stored provider.Lease
			require.NoError(t, home.DB.First(&stored, "address = ?", lease.Address).Error)
			require.Equal(t, provider.StatusClosing, stored.Status)
			require.Len(t, home.RPC.Sent(), 1)
			require.True(t, bytes.HasPrefix([]byte(home.RPC.Sent()[0].Message.Instructions[0].Data),
				dvpn.StartCloseLeaseInstructionDiscriminator[:]))
		})
	}
}

func TestRuntime_KeepLeases_ClosesStreamLeaseOfClosedNode(t *testing.T) {
	t.Parallel()

	home, r, lease, probe := newLeaseWatchFixture(t)
	rentRecipient := solana.PublicKey{2}
	seedStreamLease(t, home, lease, rentRecipient)
	home.RPC.SetAccount(home.NodeAddress, sdktest.Account{})
	require.NoError(t, home.DB.Create(&provider.RosterNode{
		Node: probe.Node,
		Plan: lease.Address,
	}).Error)
	home.RPC.Simulate = func(txn *solana.Transaction) sdktest.SimulateResult {
		if bytes.HasPrefix([]byte(txn.Message.Instructions[0].Data), dvpn.CloseLeaseInstructionDiscriminator[:]) {
			return sdktest.SimulateResult{UnitsConsumed: 5_000}
		}

		return sdktest.SimulateResult{Err: map[string]any{
			"InstructionError": []any{0, map[string]any{"Custom": 6000}},
		}}
	}

	require.NoError(t, provider.KeepLeases(t.Context(), r))

	var stored provider.Lease
	require.NoError(t, home.DB.First(&stored, "address = ?", lease.Address).Error)
	require.Equal(t, provider.StatusClosed, stored.Status)
	require.False(t, stored.Healthy)
	require.Len(t, home.RPC.Sent(), 1)
	instruction := home.RPC.Sent()[0].Message.Instructions[0]
	require.True(t, bytes.HasPrefix([]byte(instruction.Data), dvpn.CloseLeaseInstructionDiscriminator[:]))
	metas, err := instruction.ResolveInstructionAccounts(&home.RPC.Sent()[0].Message)
	require.NoError(t, err)
	require.Equal(t, home.NodeAddress, metas[5].PublicKey)
	require.Equal(t, rentRecipient, metas[6].PublicKey)
}

func TestRuntime_KeepLeases_OpensVoucherWithTrustedChainState(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	t.Cleanup(func() { require.NoError(t, r.Stop(t.Context())) })
	nodeAccount, err := home.ProgramClient.FetchNode(t.Context(), home.NodeAddress)
	require.NoError(t, err)

	const ratePerHour = uint64(3_600_000)

	node := nodeAccount.Value
	node.LeasePricePerHour = ratePerHour
	home.RPC.SetNode(home.ProgramClient.ProgramID, home.NodeAddress, node)
	require.NoError(t, home.DB.Create(&provider.RosterNode{Node: home.NodeAddress.String(), Plan: planName}).Error)
	require.NoError(t, provider.ProbeNodes(t.Context(), r))

	openedUnix := time.Now().Unix() - 30
	escrow := ratePerHour * home.Loaded.Provider.Lease.ReserveHours
	opened := make(chan solana.PublicKey, 1)
	home.RPC.Simulate = func(txn *solana.Transaction) sdktest.SimulateResult {
		for _, instruction := range txn.Message.Instructions {
			if !bytes.HasPrefix([]byte(instruction.Data), dvpn.OpenLeaseInstructionDiscriminator[:]) {
				continue
			}

			metas, parseErr := instruction.ResolveInstructionAccounts(&txn.Message)
			require.NoError(t, parseErr)

			opened <- metas[5].PublicKey
		}

		return sdktest.SimulateResult{UnitsConsumed: 5_000}
	}
	home.RPC.Server.Config.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		home.RPC.ServeHTTP(writer, request)

		select {
		case address := <-opened:
			home.RPC.SetAccount(address, sdktest.Account{
				Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, dvpn.Lease{
					Escrow: escrow, Mode: dvpn.PositionModeVoucher, Node: home.NodeAddress,
					NodeOpenedAt: node.OpenedAt, OpenedAt: openedUnix, Provider: providerAddress(t, home),
					Status: dvpn.PositionStatusOpen,
				}),
				Owner: home.ProgramClient.ProgramID,
			})
			home.NodeService.SeedLease(home.DaemonKey, address, 0, 0, escrow, ratePerHour, openedUnix)
			home.NodeService.SetLeaseUsage(address, session.Usage{Seconds: 30})
		default:
		}
	})

	require.NoError(t, provider.KeepLeases(t.Context(), r))
	require.Eventually(t, func() bool { return home.NodeService.LeaseVoucherCount() > 0 },
		2*time.Second, 10*time.Millisecond)

	var lease provider.Lease
	require.NoError(t, home.DB.First(&lease, "node = ?", home.NodeAddress.String()).Error)
	require.Equal(t, ratePerHour, lease.RatePerHour)
	require.Equal(t, openedUnix, lease.OpenedUnix)
	require.Equal(t, provider.StatusOpen, lease.Status)
	require.True(t, provider.WatcherRunning(r, lease.Address))

	var probe provider.Probe
	require.NoError(t, home.DB.First(&probe, "node = ?", home.NodeAddress.String()).Error)
	require.True(t, probe.Live)
	require.Len(t, home.RPC.Sent(), 1)
	require.Equal(
		t,
		[]solana.PublicKey{home.DaemonKey.PublicKey()},
		testutil.ParseOpenRentRecipients(t, home.RPC.Sent()[0].Message.Instructions[0].Data),
	)
}

// A node linked again while its old lease is still closing gets a new lease at once; it does not
// wait out the old lease's close grace.
func TestRuntime_KeepLeases_OpensNewLeaseWhileOldOneCloses(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	require.NoError(t, home.RPC.SetConfig(home.ProgramClient.ProgramID, dvpn.Config{
		LeaseCloseGraceSeconds: 7_200, Mint: home.ProgramClient.Mint,
	}))
	r := newRuntime(t, home)
	t.Cleanup(func() { require.NoError(t, r.Stop(t.Context())) })
	nodeAccount, err := home.ProgramClient.FetchNode(t.Context(), home.NodeAddress)
	require.NoError(t, err)

	const ratePerHour = uint64(3_600_000)

	node := nodeAccount.Value
	node.LeasePricePerHour = ratePerHour
	home.RPC.SetNode(home.ProgramClient.ProgramID, home.NodeAddress, node)

	now := time.Now().Unix()
	old := solana.PublicKey{42}
	home.RPC.SetAccount(old, sdktest.Account{
		Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, dvpn.Lease{
			CloseStartedAt: now - 60, Escrow: ratePerHour, Mode: dvpn.PositionModeVoucher, Node: home.NodeAddress,
			NodeOpenedAt: node.OpenedAt, OpenedAt: now - 3600, Provider: providerAddress(t, home),
			Status: dvpn.PositionStatusClosing,
		}),
		Owner: home.ProgramClient.ProgramID,
	})
	require.NoError(t, home.DB.Create(&provider.Lease{
		Address: old.String(), Escrow: ratePerHour, Mode: dvpn.PositionModeVoucher, Node: home.NodeAddress.String(),
		NodeOpenedUnix: node.OpenedAt, OpenedUnix: now - 3600, RatePerHour: ratePerHour,
		Status: provider.StatusClosing, UpdatedUnix: now,
	}).Error)
	require.NoError(t, provider.SyncChain(t.Context(), r)) // loads the close grace
	require.NoError(t, home.DB.Create(&provider.RosterNode{Node: home.NodeAddress.String(), Plan: planName}).Error)
	require.NoError(t, provider.ProbeNodes(t.Context(), r))

	var opens atomic.Int32
	home.RPC.Simulate = func(txn *solana.Transaction) sdktest.SimulateResult {
		for _, instruction := range txn.Message.Instructions {
			if bytes.HasPrefix([]byte(instruction.Data), dvpn.OpenLeaseInstructionDiscriminator[:]) {
				opens.Add(1)
			}
		}

		return sdktest.SimulateResult{UnitsConsumed: 5_000}
	}

	_ = provider.KeepLeases(t.Context(), r) // the fake chain never creates the new lease account
	require.Equal(t, int32(1), opens.Load(), "a new lease is opened while the old one closes")

	var oldRow provider.Lease
	require.NoError(t, home.DB.First(&oldRow, "address = ?", old.String()).Error)
	require.Equal(t, provider.StatusClosing, oldRow.Status, "the old lease keeps closing on its own")
}

func TestRuntime_KeepLeases_PreservesZeroPriceAndRetiresStaleEpoch(t *testing.T) {
	t.Parallel()

	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "zero price", true: "stale epoch"}[stale], func(t *testing.T) {
			t.Parallel()

			home, r, lease, probe := newLeaseWatchFixture(t)
			node, err := home.ProgramClient.FetchNode(t.Context(), home.NodeAddress)
			require.NoError(t, err)

			lease.RatePerHour = 0
			if stale {
				lease.NodeOpenedUnix--
			}

			require.NoError(t, home.DB.Save(&lease).Error)
			address := solana.MustPublicKeyFromBase58(lease.Address)
			chainLease := dvpn.Lease{
				Escrow: lease.Escrow, Mode: lease.Mode, Node: home.NodeAddress, NodeOpenedAt: lease.NodeOpenedUnix,
				OpenedAt: lease.OpenedUnix, Provider: providerAddress(t, home), Status: dvpn.PositionStatusOpen,
			}
			home.RPC.SetAccount(address, sdktest.Account{
				Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, chainLease), Owner: home.ProgramClient.ProgramID,
			})
			home.NodeService.SeedLease(home.DaemonKey, address, 0, 0, lease.Escrow, 0, lease.OpenedUnix)
			require.NoError(t, home.DB.Create(&provider.RosterNode{Node: probe.Node, Plan: planName}).Error)

			node.Value.LeasePricePerHour = 3_600_000
			home.RPC.SetNode(home.ProgramClient.ProgramID, home.NodeAddress, node.Value)

			require.NoError(t, provider.KeepLeases(t.Context(), r))

			var stored provider.Lease
			require.NoError(t, home.DB.First(&stored, "address = ?", lease.Address).Error)
			require.Zero(t, stored.RatePerHour)

			if stale {
				require.Equal(t, provider.StatusClosing, stored.Status)
				require.False(t, provider.WatcherRunning(r, lease.Address))
				require.Zero(t, home.NodeService.LeaseVoucherCount())
				require.Len(t, home.RPC.Sent(), 1)
				require.True(t, bytes.HasPrefix([]byte(home.RPC.Sent()[0].Message.Instructions[0].Data),
					dvpn.StartCloseLeaseInstructionDiscriminator[:]))
			} else {
				require.Equal(t, provider.StatusOpen, stored.Status)
				require.Eventually(t, func() bool { return home.NodeService.StreamOpenCount() > 0 },
					2*time.Second, 10*time.Millisecond)
				require.True(t, provider.WatcherRunning(r, lease.Address))
				require.Empty(t, home.RPC.Sent())
			}
		})
	}
}

func TestRuntime_KeepLeases_ProbesRosterNodeWithoutProbeRow(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	t.Cleanup(func() { require.NoError(t, r.Stop(t.Context())) })
	require.NoError(t, home.DB.Create(&provider.RosterNode{
		Node: home.NodeAddress.String(),
		Plan: planName,
	}).Error)
	home.NodeServer.Close()

	require.NoError(t, provider.KeepLeases(t.Context(), r))
	require.Empty(t, home.RPC.Sent())

	var probe provider.Probe
	require.NoError(t, home.DB.First(&probe, "node = ?", home.NodeAddress.String()).Error)
	require.Positive(t, probe.ProbedUnix)
	require.False(t, probe.Live)
}

func TestRuntime_KeepLeases_ProbesStaleRosterFirst(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	t.Cleanup(func() { require.NoError(t, r.Stop(t.Context())) })

	probe := provider.Probe{
		Live: true,
		Node: home.NodeAddress.String(),
	}
	require.NoError(t, home.DB.Create(&probe).Error)
	require.NoError(t, home.DB.Create(&provider.RosterNode{
		Node: home.NodeAddress.String(),
		Plan: planName,
	}).Error)
	home.NodeServer.Close()

	require.NoError(t, provider.KeepLeases(t.Context(), r))
	require.Empty(t, home.RPC.Sent())
	require.NoError(t, home.DB.First(&probe, "node = ?", home.NodeAddress.String()).Error)
	require.Positive(t, probe.ProbedUnix)
	require.False(t, probe.Live)
}

func TestRuntime_KeepLeases_RejectsProbeAgedDuringPass(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	t.Cleanup(func() { require.NoError(t, r.Stop(t.Context())) })
	require.NoError(t, home.DB.Create(&provider.Probe{
		Live: true,
		Node: home.NodeAddress.String(),
	}).Error)
	require.NoError(t, home.DB.Create(&provider.RosterNode{
		Node: home.NodeAddress.String(),
		Plan: planName,
	}).Error)

	var checkedUnix int64

	require.NoError(t, home.DB.Callback().Query().After("gorm:query").Register("provider_test:age_probe",
		func(db *gorm.DB) {
			switch dest := db.Statement.Dest.(type) {
			case *[]provider.Probe:
				if checkedUnix == 0 && len(*dest) > 0 {
					checkedUnix = time.Now().Unix()
					(*dest)[0].ProbedUnix = checkedUnix - 2*int64(provider.ProbeNodesInterval.Seconds()) + 1
				}
			case *provider.Lease:
				if checkedUnix != 0 {
					time.Sleep(time.Until(time.Unix(checkedUnix+2, 0)))
				}
			}
		}))

	require.NoError(t, provider.KeepLeases(t.Context(), r))
	require.Empty(t, home.RPC.Sent())
}

func TestRuntime_KeepLeases_ReportsActionFailures(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	t.Cleanup(func() { require.NoError(t, r.Stop(t.Context())) })
	require.NoError(t, home.DB.Create(&provider.RosterNode{
		Node: home.NodeAddress.String(),
		Plan: planName,
	}).Error)
	require.NoError(t, provider.ProbeNodes(t.Context(), r))

	secondAddress := solana.PublicKey{44}
	nodeAccount, err := home.ProgramClient.FetchNode(t.Context(), home.NodeAddress)
	require.NoError(t, err)
	home.RPC.SetNode(home.ProgramClient.ProgramID, secondAddress, nodeAccount.Value)

	var secondProbe provider.Probe
	require.NoError(t, home.DB.First(&secondProbe, "node = ?", home.NodeAddress.String()).Error)
	secondProbe.Node = secondAddress.String()
	require.NoError(t, home.DB.Create(&secondProbe).Error)
	require.NoError(t, home.DB.Create(&provider.RosterNode{
		Node: secondAddress.String(),
		Plan: planName,
	}).Error)
	home.RPC.Simulate = func(*solana.Transaction) sdktest.SimulateResult {
		return sdktest.SimulateResult{Err: map[string]any{
			"InstructionError": []any{0, map[string]any{"Custom": 6000}},
		}}
	}

	err = provider.KeepLeases(t.Context(), r)
	require.ErrorContains(t, err, "open")
	require.ErrorContains(t, err, home.NodeAddress.String())
	require.ErrorContains(t, err, secondAddress.String())
}

func TestRuntime_KeepLeases_ReportsFailedClose(t *testing.T) {
	t.Parallel()

	home, r, lease, probe := newLeaseWatchFixture(t)
	address := solana.MustPublicKeyFromBase58(lease.Address)
	home.RPC.SetAccount(address, sdktest.Account{
		Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, dvpn.Lease{
			Escrow:       lease.Escrow,
			Mode:         lease.Mode,
			Node:         home.NodeAddress,
			NodeOpenedAt: lease.NodeOpenedUnix,
			OpenedAt:     lease.OpenedUnix,
			Provider:     providerAddress(t, home),
			Status:       dvpn.PositionStatusOpen,
		}),
		Owner: home.ProgramClient.ProgramID,
	})
	require.NoError(t, home.DB.Create(&provider.RosterNode{
		Node: probe.Node,
		Plan: planName,
	}).Error)
	home.RPC.SetAccount(home.NodeAddress, sdktest.Account{})
	home.RPC.Simulate = func(*solana.Transaction) sdktest.SimulateResult {
		return sdktest.SimulateResult{Err: map[string]any{
			"InstructionError": []any{0, map[string]any{"Custom": 6000}},
		}}
	}

	err := provider.KeepLeases(t.Context(), r)
	require.ErrorContains(t, err, "closing lease of closed node")

	var stored provider.Lease
	require.NoError(t, home.DB.First(&stored, "address = ?", lease.Address).Error)
	require.Equal(t, provider.StatusOpen, stored.Status)
}

func TestRuntime_KeepLeases_ReprobesRecentLiveNodeAfterStart(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	t.Cleanup(func() { require.NoError(t, r.Stop(t.Context())) })
	require.NoError(t, home.DB.Create(&provider.RosterNode{
		Node: home.NodeAddress.String(),
		Plan: planName,
	}).Error)
	require.NoError(t, provider.ProbeNodes(t.Context(), r))

	var probe provider.Probe
	require.NoError(t, home.DB.First(&probe, "node = ?", home.NodeAddress.String()).Error)
	require.True(t, probe.Live)
	require.Positive(t, probe.ProbedUnix)
	home.NodeServer.Close()

	require.NoError(t, r.Start(t.Context()))
	require.NoError(t, provider.KeepLeases(t.Context(), r))
	require.Empty(t, home.RPC.Sent())
	require.NoError(t, home.DB.First(&probe, "node = ?", home.NodeAddress.String()).Error)
	require.False(t, probe.Live)
}

func TestRuntime_KeepLeases_SerializesConcurrentRuns(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	t.Cleanup(func() { require.NoError(t, r.Stop(t.Context())) })
	nodeAccount, err := home.ProgramClient.FetchNode(t.Context(), home.NodeAddress)
	require.NoError(t, err)

	const ratePerHour = uint64(3_600_000)

	node := nodeAccount.Value
	node.LeasePricePerHour = ratePerHour
	home.RPC.SetNode(home.ProgramClient.ProgramID, home.NodeAddress, node)
	require.NoError(t, home.DB.Create(&provider.RosterNode{
		Node: home.NodeAddress.String(),
		Plan: planName,
	}).Error)
	require.NoError(t, provider.ProbeNodes(t.Context(), r))

	firstSending := make(chan struct{})
	releaseFirst := make(chan struct{})
	opened := make(chan solana.PublicKey, 1)
	secondEntered := make(chan bool, 1)

	home.RPC.Simulate = func(txn *solana.Transaction) sdktest.SimulateResult {
		for _, instruction := range txn.Message.Instructions {
			if !bytes.HasPrefix([]byte(instruction.Data), dvpn.OpenLeaseInstructionDiscriminator[:]) {
				continue
			}

			metas, parseErr := instruction.ResolveInstructionAccounts(&txn.Message)
			require.NoError(t, parseErr)

			select {
			case opened <- metas[5].PublicKey:
			default:
			}
		}

		return sdktest.SimulateResult{UnitsConsumed: 5_000}
	}
	home.RPC.Server.Config.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)

			return
		}

		request.Body = io.NopCloser(bytes.NewReader(body))

		var rpcRequest struct {
			Method string `json:"method"`
		}
		if err := json.Unmarshal(body, &rpcRequest); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)

			return
		}

		if rpcRequest.Method == "sendTransaction" {
			select {
			case <-firstSending:
			default:
				close(firstSending)
				<-releaseFirst

				address := <-opened
				openedUnix := time.Now().Unix() - 30
				escrow := ratePerHour * home.Loaded.Provider.Lease.ReserveHours
				home.RPC.SetAccount(address, sdktest.Account{
					Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, dvpn.Lease{
						Escrow:       escrow,
						Mode:         dvpn.PositionModeVoucher,
						Node:         home.NodeAddress,
						NodeOpenedAt: node.OpenedAt,
						OpenedAt:     openedUnix,
						Provider:     providerAddress(t, home),
						Status:       dvpn.PositionStatusOpen,
					}),
					Owner: home.ProgramClient.ProgramID,
				})
				home.NodeService.SeedLease(home.DaemonKey, address, 0, 0, escrow, ratePerHour, openedUnix)
				home.RPC.ServeHTTP(writer, request)

				return
			}
		}

		home.RPC.ServeHTTP(writer, request)
	})

	results := make(chan error, 2)
	go func() { results <- provider.KeepLeases(t.Context(), r) }()

	<-firstSending

	go func() {
		secondEntered <- provider.KeepRunning(r)

		results <- provider.KeepLeases(t.Context(), r)
	}()

	var locked bool
	select {
	case locked = <-secondEntered:
	case <-time.After(2 * time.Second):
		close(releaseFirst)
		t.Fatal("second keep invocation did not start")
	}

	close(releaseFirst)
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	require.True(t, locked, "first keep pass must hold the lock while sending")

	var opens int

	for _, txn := range home.RPC.Sent() {
		for _, instruction := range txn.Message.Instructions {
			if bytes.HasPrefix([]byte(instruction.Data), dvpn.OpenLeaseInstructionDiscriminator[:]) {
				opens++
			}
		}
	}

	require.Equal(t, 1, opens)
}

func TestRuntime_KeepLeases_WatchesLiveVoucherLeases(t *testing.T) {
	t.Parallel()

	home, r, lease, probe := newLeaseWatchFixture(t)
	address := solana.MustPublicKeyFromBase58(lease.Address)
	chainLease := dvpn.Lease{
		Escrow: lease.Escrow, Mode: lease.Mode, Node: home.NodeAddress,
		NodeOpenedAt: lease.NodeOpenedUnix, OpenedAt: lease.OpenedUnix, Provider: providerAddress(t, home),
		RatePerHour: lease.RatePerHour, Status: dvpn.PositionStatusOpen,
	}
	home.RPC.SetAccount(address, sdktest.Account{
		Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, chainLease), Owner: home.ProgramClient.ProgramID,
	})
	require.NoError(t, home.DB.Create(&provider.RosterNode{Node: probe.Node, Plan: planName}).Error)

	require.NoError(t, provider.KeepLeases(t.Context(), r))
	require.True(t, provider.WatcherRunning(r, lease.Address))
	require.NoError(t, home.DB.Model(&provider.Probe{}).Where("node = ?", probe.Node).Update("live", false).Error)
	require.NoError(t, provider.KeepLeases(t.Context(), r))
	require.False(t, provider.WatcherRunning(r, lease.Address))

	require.NoError(t, home.DB.Model(&provider.Probe{}).Where("node = ?", probe.Node).Update("live", true).Error)
	require.NoError(t, provider.KeepLeases(t.Context(), r))
	require.True(t, provider.WatcherRunning(r, lease.Address))

	chainLease.Status = dvpn.PositionStatusClosing
	chainLease.CloseStartedAt = time.Now().Unix() + 3600
	home.RPC.SetAccount(address, sdktest.Account{
		Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, chainLease), Owner: home.ProgramClient.ProgramID,
	})
	// The node is still a live roster member, so a new lease is opened beside the closing one; the
	// fake chain never creates that account, hence the error.
	require.ErrorContains(t, provider.KeepLeases(t.Context(), r), "fetching opened lease")
	require.False(t, provider.WatcherRunning(r, lease.Address))

	require.NoError(t, r.Stop(t.Context()))

	chainLease.Status = dvpn.PositionStatusOpen
	home.RPC.SetAccount(address, sdktest.Account{
		Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, chainLease), Owner: home.ProgramClient.ProgramID,
	})
	require.NoError(t, provider.KeepLeases(t.Context(), r))
	require.False(t, provider.WatcherRunning(r, lease.Address))
}

func TestRuntime_ProbeNodes_RecordsSettlement(t *testing.T) {
	t.Parallel()

	home := testutil.NewProviderHome(t, sdktest.NewFakeRPC(t))
	r := newRuntime(t, home)
	t.Cleanup(func() { require.NoError(t, r.Stop(t.Context())) })
	require.NoError(t, home.DB.Create(&provider.RosterNode{Node: home.NodeAddress.String(), Plan: planName}).Error)

	require.NoError(t, provider.ProbeNodes(t.Context(), r))

	var probe provider.Probe
	require.NoError(t, home.DB.First(&probe, "node = ?", home.NodeAddress.String()).Error)
	require.True(t, probe.Live)
	require.EqualValues(t, 1, probe.HeartbeatSeconds)
	require.EqualValues(t, 3600, probe.MinLeasePrepaySeconds)
}

func TestRuntime_StartCloseLease_DeduplicatesConcurrentRequests(t *testing.T) {
	t.Parallel()
	home, r, lease, _ := newLeaseWatchFixture(t)
	results := make(chan error, 2)

	for range 2 {
		go func() {
			closing := lease
			results <- provider.StartCloseLease(t.Context(), r, &closing)
		}()
	}

	for range 2 {
		require.NoError(t, <-results)
	}

	require.Len(t, home.RPC.Sent(), 1)

	var stored provider.Lease
	require.NoError(t, home.DB.First(&stored, "address = ?", lease.Address).Error)
	require.Equal(t, provider.StatusClosing, stored.Status)
}

func TestRuntime_StartCloseLease_RetriesFailedTransaction(t *testing.T) {
	t.Parallel()
	home, r, lease, _ := newLeaseWatchFixture(t)
	home.RPC.Simulate = func(*solana.Transaction) sdktest.SimulateResult {
		return sdktest.SimulateResult{
			Err:  map[string]any{"InstructionError": []any{0, map[string]any{"Custom": 6000}}},
			Logs: []string{"Program log: AnchorError. Error Code: Unauthorized. Error Number: 6000."},
		}
	}

	require.Error(t, provider.StartCloseLease(t.Context(), r, &lease))

	var stored provider.Lease
	require.NoError(t, home.DB.First(&stored, "address = ?", lease.Address).Error)
	require.Equal(t, provider.StatusOpen, stored.Status)

	home.RPC.Simulate = nil

	require.NoError(t, provider.StartCloseLease(t.Context(), r, &lease))
	require.Len(t, home.RPC.Sent(), 1)
	require.NoError(t, home.DB.First(&stored, "address = ?", lease.Address).Error)
	require.Equal(t, provider.StatusClosing, stored.Status)
}

func TestRuntime_StartLeaseWatcher_RejectsStaleNodeEpoch(t *testing.T) {
	t.Parallel()

	home, r, lease, probe := newLeaseWatchFixture(t)
	lease.NodeOpenedUnix--
	require.NoError(t, home.DB.Save(&lease).Error)

	provider.StartLeaseWatcher(t.Context(), r, lease, probe)
	require.Eventually(t, func() bool {
		var stored provider.Lease

		return home.DB.First(&stored, "address = ?", lease.Address).Error == nil && stored.Status == provider.StatusClosing
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, home.NodeService.LeaseVoucherCount())
	require.Zero(t, home.NodeService.LeaseReadCount())
}

func closeLeaseGrants(t *testing.T, sent []*solana.Transaction) map[solana.PublicKey]solana.PublicKey {
	t.Helper()

	grants := make(map[solana.PublicKey]solana.PublicKey, len(sent))
	for _, txn := range sent {
		instruction := txn.Message.Instructions[0]
		require.True(t, bytes.HasPrefix([]byte(instruction.Data), dvpn.CloseLeaseInstructionDiscriminator[:]))

		metas, err := instruction.ResolveInstructionAccounts(&txn.Message)
		require.NoError(t, err)
		require.Len(t, metas, 7)

		grants[metas[3].PublicKey] = metas[4].PublicKey
	}

	return grants
}

func seedStreamLease(t *testing.T, home testutil.Home, lease provider.Lease, rentRecipient solana.PublicKey) {
	t.Helper()

	chainLease := dvpn.Lease{
		Escrow:          lease.Escrow,
		Mode:            dvpn.PositionModeStream,
		Node:            home.NodeAddress,
		NodeOpenedAt:    lease.NodeOpenedUnix,
		OpenedAt:        lease.OpenedUnix,
		Owner:           home.OwnerAddress,
		Provider:        providerAddress(t, home),
		RatePerHour:     lease.RatePerHour,
		RentRecipient:   rentRecipient,
		Status:          dvpn.PositionStatusOpen,
		StreamStartedAt: lease.OpenedUnix,
	}
	home.RPC.SetAccount(solana.MustPublicKeyFromBase58(lease.Address), sdktest.Account{
		Data:  sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, chainLease),
		Owner: home.ProgramClient.ProgramID,
	})
}
