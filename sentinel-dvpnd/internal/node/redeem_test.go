//go:build linux

package node_test

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	token2022 "github.com/solana-foundation/solana-go/v2/programs/token-2022"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/node"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/paymaster"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/voucher"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

func TestRuntime_Redeem_BatchesWithinConfiguredTransactionLimit(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	home.Sender.Config.MaxTransactionBytes = 500
	owners := make([]solana.PrivateKey, 1, 3)
	owners[0] = home.OwnerKey

	for range 2 {
		owner, err := solana.NewRandomPrivateKey()
		require.NoError(t, err)

		owners = append(owners, owner)
	}

	for _, owner := range owners {
		address := seedChannel(t, r, home, owner, dvpn.PositionModeVoucher)
		seedPosition(t, r, home, address, home.Loaded.Node.Redeem.ThresholdBaseUnits*2)
	}

	require.NoError(t, node.Redeem(t.Context(), r))
	require.Len(t, home.RPC.Sent(), 2)
}

func TestRuntime_Redeem_DoesNotRetrySingleSubject(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	address := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	seedPosition(t, r, home, address, home.Loaded.Node.Redeem.ThresholdBaseUnits*2)

	var attempts atomic.Int32

	home.RPC.Simulate = func(*solana.Transaction) sdktest.SimulateResult {
		attempts.Add(1)

		return sdktest.SimulateResult{Err: "simulated program error"}
	}

	require.NoError(t, node.Redeem(t.Context(), r))
	require.EqualValues(t, 1, attempts.Load())

	var redemptions []node.Redemption
	require.NoError(t, home.DB.Find(&redemptions).Error)
	require.Len(t, redemptions, 1)
	require.Equal(t, node.RedemptionStatusFailed, redemptions[0].Status)
}

func TestRuntime_Redeem_PaymasterLeavesRoomForFeeTransfer(t *testing.T) {
	t.Parallel()

	cases := []struct {
		maxBytes int
		name     string
	}{
		{maxBytes: program.DefaultMaxTransactionBytes, name: "default limit"},
		{maxBytes: 2 * solana.MaxTransactionSizeV1, name: "above the transaction size limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r, home, _ := newRuntime(t)
			paymasterKey, err := solana.NewRandomPrivateKey()
			require.NoError(t, err)

			home.Sender.Config.MaxTransactionBytes = tc.maxBytes
			home.Sender.Paymaster = sdktest.FakePaymaster{
				Key:      paymasterKey,
				Transfer: newFeeTransfer(t, home, paymasterKey.PublicKey()),
			}
			maxBytes := min(tc.maxBytes, solana.MaxTransactionSizeV1)
			total := program.CapacityFor(program.BatchRedeemVouchers, maxBytes)
			capacity := program.CapacityFor(program.BatchRedeemVouchers, maxBytes-paymaster.FeeTransferOverheadBytes)
			subjects := map[solana.PublicKey]bool{}

			for range total {
				owner, err := solana.NewRandomPrivateKey()
				require.NoError(t, err)

				address := seedChannel(t, r, home, owner, dvpn.PositionModeVoucher)
				seedPosition(t, r, home, address, home.Loaded.Node.Redeem.ThresholdBaseUnits*2)
				subjects[address] = true
			}

			require.NoError(t, node.Redeem(t.Context(), r))

			sent := home.RPC.Sent()
			counts := make([]int, 0, len(sent))

			for _, transaction := range sent {
				raw, err := transaction.MarshalBinary()
				require.NoError(t, err)
				require.Len(t, transaction.Message.Instructions, 2)
				require.LessOrEqual(t, len(raw), maxBytes)

				accounts, err := resolveAccounts(transaction)
				require.NoError(t, err)

				count := 0

				for _, account := range accounts {
					if subjects[account] {
						count++
					}
				}

				limit := transaction.Message.TransactionConfig.ComputeUnitLimit
				require.NotNil(t, limit)
				require.Equal(t, program.ComputeUnits(program.BatchRedeemVouchers, count)+paymaster.FeeTransferComputeUnits, *limit)

				counts = append(counts, count)
			}

			require.Equal(t, []int{capacity, total - capacity}, counts)
		})
	}
}

func TestRuntime_Redeem_PreservesReopenedPosition(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	address := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	home.Loaded.Node.Redeem.ThresholdBaseUnits = 1
	signed := voucher.Sign(home.OwnerKey, voucher.ChannelMessage(address, channelOpenedUnix, 200))
	_, err := node.AcceptChannelVoucher(t.Context(), r, address, home.OwnerAddress, nodeapi.Voucher{
		Cumulative: 200,
		Signature:  signed.Signature[:],
	})
	require.NoError(t, err)

	var reopened atomic.Bool

	require.NoError(t, home.DB.Callback().Update().Before("gorm:begin_transaction").
		Register("test:reopen", func(db *gorm.DB) {
			updates, ok := db.Statement.Dest.(map[string]any)
			if !ok || updates["redeemed_unix"] == nil || updates["chain_slot"] != nil || reopened.Swap(true) {
				return
			}

			_, channel := channelFixture(
				t, r, home, home.OwnerKey, home.OwnerAddress, home.OwnerAddress, dvpn.PositionModeVoucher,
			)
			channel.OpenedAt++
			home.RPC.SetAccount(address, sdktest.Account{
				Data:  sdktest.EncodeAccount(dvpn.ChannelAccountDiscriminator, channel),
				Owner: home.ProgramClient.ProgramID,
			})
			_, refreshErr := node.NewHandler(r).GetChannel(
				newSignedContext(t, home.OwnerAddress), nodeapi.GetChannelRequestObject{Channel: address},
			)
			require.NoError(t, refreshErr)
		}))
	require.NoError(t, node.Redeem(t.Context(), r))
	response, err := node.NewHandler(r).GetChannel(
		newSignedContext(t, home.OwnerAddress), nodeapi.GetChannelRequestObject{Channel: address},
	)
	require.NoError(t, err)

	view, ok := response.(nodeapi.GetChannel200JSONResponse)
	require.True(t, ok)
	require.Equal(t, channelOpenedUnix+1, view.Ledger.OpenedAt)
	require.Zero(t, view.Ledger.CumulativeBaseUnits)
	require.Zero(t, view.Ledger.RedeemedBaseUnits)
	require.Empty(t, view.Ledger.Signature)

	var stored node.Position
	require.NoError(t, home.DB.WithContext(t.Context()).First(&stored, "address = ?", address.String()).Error)
	require.Zero(t, stored.AcceptedUnix)
	require.Zero(t, stored.RedeemedUnix)
	require.Zero(t, stored.Settled)
}

func TestRuntime_Redeem_RedeemsClosingBelowThreshold(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	address := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	cumulative := home.Loaded.Node.Redeem.ThresholdBaseUnits / 2
	seedPosition(t, r, home, address, cumulative)
	require.NoError(t, home.DB.Model(&node.Position{}).Where("address = ?", address.String()).Updates(map[string]any{
		"accepted_unix": time.Now().Unix(),
		"status":        node.PositionStatusClosing,
	}).Error)

	require.NoError(t, node.Redeem(t.Context(), r))
	require.Len(t, home.RPC.Sent(), 1)

	var position node.Position
	require.NoError(t, home.DB.First(&position, "address = ?", address.String()).Error)
	require.Equal(t, cumulative, position.Redeemed)
}

func TestRuntime_Redeem_RetriesFailedBatchAsSingles(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	ownerB, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	addressA := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	addressB := seedChannel(t, r, home, ownerB, dvpn.PositionModeVoucher)
	cumulative := home.Loaded.Node.Redeem.ThresholdBaseUnits * 2
	seedPosition(t, r, home, addressA, cumulative)
	seedPosition(t, r, home, addressB, cumulative)
	subjects := map[solana.PublicKey]bool{addressA: true, addressB: true}

	var (
		mu       sync.Mutex
		attempts int
		simErr   error
	)

	home.RPC.Simulate = func(sentTx *solana.Transaction) sdktest.SimulateResult {
		accounts, resolveErr := resolveAccounts(sentTx)

		mu.Lock()
		attempts++

		if resolveErr != nil && simErr == nil {
			simErr = resolveErr
		}
		mu.Unlock()

		matched := 0

		for _, address := range accounts {
			if subjects[address] {
				matched++
			}
		}

		if matched == 2 {
			return sdktest.SimulateResult{Err: "simulated program error"}
		}

		return sdktest.SimulateResult{UnitsConsumed: 5_000}
	}

	require.NoError(t, node.Redeem(t.Context(), r))
	mu.Lock()
	gotAttempts, gotErr := attempts, simErr
	mu.Unlock()
	require.NoError(t, gotErr)
	require.Equal(t, 3, gotAttempts)
	require.Len(t, home.RPC.Sent(), 2)

	var positionA, positionB node.Position
	require.NoError(t, home.DB.WithContext(t.Context()).First(&positionA, "address = ?", addressA.String()).Error)
	require.NoError(t, home.DB.WithContext(t.Context()).First(&positionB, "address = ?", addressB.String()).Error)
	require.Equal(t, cumulative, positionA.Redeemed)
	require.Equal(t, cumulative, positionB.Redeemed)

	var redemptions []node.Redemption
	require.NoError(t, home.DB.WithContext(t.Context()).Find(&redemptions).Error)
	require.Len(t, redemptions, 3)

	failed, confirmed := 0, 0

	for _, row := range redemptions {
		switch row.Status {
		case node.RedemptionStatusFailed:
			failed++
		case node.RedemptionStatusConfirmed:
			confirmed++
		}
	}

	require.Equal(t, 1, failed)
	require.Equal(t, 2, confirmed)
}

func TestRuntime_Redeem_SendsBatchAndAdvancesPositions(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	ownerB, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	addressA := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	addressB := seedChannel(t, r, home, ownerB, dvpn.PositionModeVoucher)
	cumulative := home.Loaded.Node.Redeem.ThresholdBaseUnits * 2
	seedPosition(t, r, home, addressA, cumulative)
	seedPosition(t, r, home, addressB, cumulative)
	require.NoError(t, node.Redeem(t.Context(), r))

	sent := home.RPC.Sent()
	require.Len(t, sent, 1)
	accounts, err := resolveAccounts(sent[0])
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(accounts), 2)

	subjects := []solana.PublicKey{addressA, addressB}
	want := program.SortedUniqueSubjects(subjects)
	got := accounts[len(accounts)-2:]
	require.Equal(t, want, got)

	metas, err := sent[0].Message.Instructions[0].ResolveInstructionAccounts(&sent[0].Message)
	require.NoError(t, err)

	for _, meta := range metas[len(metas)-2:] {
		require.True(t, meta.IsWritable)
		require.False(t, meta.IsSigner)
	}

	var positionA, positionB node.Position
	require.NoError(t, home.DB.WithContext(t.Context()).First(&positionA, "address = ?", addressA.String()).Error)
	require.NoError(t, home.DB.WithContext(t.Context()).First(&positionB, "address = ?", addressB.String()).Error)
	require.Equal(t, cumulative, positionA.Redeemed)
	require.Equal(t, cumulative, positionB.Redeemed)

	var redemptions []node.Redemption
	require.NoError(t, home.DB.WithContext(t.Context()).Find(&redemptions).Error)
	require.Len(t, redemptions, 1)
	require.Equal(t, node.RedemptionStatusConfirmed, redemptions[0].Status)
}

func TestRuntime_Redeem_SkipsSweepWithoutFees(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	home.Loaded.Node.Redeem.ThresholdBaseUnits = 0
	chainCfg, _ := node.ChainState(r)
	chainCfg.Treasury = solana.PublicKey{99}
	require.NoError(t, home.RPC.SetConfig(home.ProgramClient.ProgramID, chainCfg))
	require.NoError(t, node.SyncChain(t.Context(), r))
	_, chainNode := node.ChainState(r)
	chainNode.FeesAccrued = 10
	node.SetChainNode(r, chainNode)

	require.NoError(t, node.Redeem(t.Context(), r))

	for _, sent := range home.RPC.Sent() {
		for _, instruction := range sent.Message.Instructions {
			require.False(t, bytes.HasPrefix(instruction.Data, dvpn.SweepNodeFeesInstructionDiscriminator[:]))
		}
	}
}

func newFeeTransfer(t *testing.T, home testutil.Home, paymasterAddress solana.PublicKey) sdktest.FakeFeeTransfer {
	t.Helper()

	source, _, err := solana.FindAssociatedTokenAddressWithProgram(
		home.DaemonKey.PublicKey(), home.ProgramClient.Mint, solana.Token2022ProgramID,
	)
	require.NoError(t, err)

	destination, _, err := solana.FindAssociatedTokenAddressWithProgram(
		paymasterAddress, home.ProgramClient.Mint, solana.Token2022ProgramID,
	)
	require.NoError(t, err)

	transfer := token2022.NewTransferCheckedInstruction(
		1, 6, source, home.ProgramClient.Mint, destination, home.DaemonKey.PublicKey(), nil,
	).Build()

	return sdktest.FakeFeeTransfer{
		Placeholders: []solana.Instruction{transfer},
		Quoted:       []solana.Instruction{transfer},
	}
}

func TestRuntime_Redeem_ConcurrentPassesRedeemOnce(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	address := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	cumulative := home.Loaded.Node.Redeem.ThresholdBaseUnits * 2
	seedPosition(t, r, home, address, cumulative)
	// Settled up to the voucher, so the whole amount is due whichever way the claim is measured.
	require.NoError(t, home.DB.Model(&node.Position{}).Where("address = ?", address.String()).
		Update("settled", cumulative).Error)

	// The hourly job, position sync and RedeemNow all call the same pass; start four at once.
	const passes = 4

	var wg sync.WaitGroup

	start := make(chan struct{})
	errs := make([]error, passes)

	for i := range passes {
		wg.Go(func() {
			<-start

			errs[i] = node.Redeem(t.Context(), r)
		})
	}

	close(start)
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	require.Len(t, home.RPC.Sent(), 1, "the due position is redeemed by one transaction")

	var redemptions []node.Redemption
	require.NoError(t, home.DB.WithContext(t.Context()).Find(&redemptions).Error)
	require.Len(t, redemptions, 1)
	require.Equal(t, node.RedemptionStatusConfirmed, redemptions[0].Status)

	var position node.Position
	require.NoError(t, home.DB.WithContext(t.Context()).First(&position, "address = ?", address.String()).Error)
	require.Equal(t, cumulative, position.Redeemed, "the position is advanced, not left due")
}
