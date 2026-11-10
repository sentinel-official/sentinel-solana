//go:build linux

package node

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"

	"github.com/sentinel-official/sentinel-go-sdk/v3/paymaster"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	pendingRedemptionTTL = 5 * time.Minute
	redeemLabel          = "redeem"
	resultError          = "error"
	resultOK             = "ok"
)

var errMissingTreasury = errors.New("missing treasury")

type redeemGroup struct {
	key          string
	kind         program.BatchKind
	mode         uint8
	positionKind string
	subjects     map[solana.PublicKey]redeemSubject
}

type redeemSubject struct {
	nodeOpenedUnix int64
	openedUnix     int64
	voucher        dvpn.Voucher
}

func (r *Runtime) buildRedeemInstruction(group *redeemGroup, subjects []solana.PublicKey) (solana.Instruction, error) {
	sorted := program.SortedUniqueSubjects(subjects)
	remaining := r.programClient.RemainingAccounts(sorted...)

	switch group.key {
	case groupKey(KindChannel, dvpn.PositionModeVoucher):
		vouchers := make([]dvpn.Voucher, 0, len(sorted))
		for _, subject := range sorted {
			vouchers = append(vouchers, group.subjects[subject].voucher)
		}

		params := dvpn.RedeemChannelVouchersParams{
			Node:              r.address,
			RemainingAccounts: remaining,
			Signer:            r.signer,
			Vouchers:          vouchers,
		}

		instruction, err := dvpn.BuildRedeemChannelVouchersInstruction(r.programClient.ProgramID, params)
		if err != nil {
			return nil, fmt.Errorf("building redeem_channel_vouchers instruction: %w", err)
		}

		return instruction, nil
	case groupKey(KindChannel, dvpn.PositionModeStream):
		params := dvpn.RedeemChannelStreamsParams{
			Node:              r.address,
			RemainingAccounts: remaining,
			Signer:            r.signer,
		}

		instruction, err := dvpn.BuildRedeemChannelStreamsInstruction(r.programClient.ProgramID, params)
		if err != nil {
			return nil, fmt.Errorf("building redeem_channel_streams instruction: %w", err)
		}

		return instruction, nil
	case groupKey(KindLease, dvpn.PositionModeVoucher):
		vouchers := make([]dvpn.Voucher, 0, len(sorted))
		for _, subject := range sorted {
			vouchers = append(vouchers, group.subjects[subject].voucher)
		}

		params := dvpn.RedeemLeaseVouchersParams{
			Node:              r.address,
			RemainingAccounts: remaining,
			Signer:            r.signer,
			Vouchers:          vouchers,
		}

		instruction, err := dvpn.BuildRedeemLeaseVouchersInstruction(r.programClient.ProgramID, params)
		if err != nil {
			return nil, fmt.Errorf("building redeem_lease_vouchers instruction: %w", err)
		}

		return instruction, nil
	case groupKey(KindLease, dvpn.PositionModeStream):
		params := dvpn.RedeemLeaseStreamsParams{
			Node:              r.address,
			RemainingAccounts: remaining,
			Signer:            r.signer,
		}

		instruction, err := dvpn.BuildRedeemLeaseStreamsInstruction(r.programClient.ProgramID, params)
		if err != nil {
			return nil, fmt.Errorf("building redeem_lease_streams instruction: %w", err)
		}

		return instruction, nil
	default:
		return nil, fmt.Errorf("unknown redemption group %s", group.key)
	}
}

func (r *Runtime) buildSweepInstruction(chainCfg dvpn.Config) (solana.Instruction, error) {
	if chainCfg.Treasury.IsZero() {
		return nil, errMissingTreasury
	}

	vault, err := r.programClient.VaultAddress()
	if err != nil {
		return nil, err
	}

	return dvpn.BuildSweepNodeFeesInstruction(r.programClient.ProgramID, dvpn.SweepNodeFeesParams{
		Mint:              r.programClient.Mint,
		RemainingAccounts: r.programClient.RemainingAccounts(r.address),
		Signer:            r.signer,
		Treasury:          chainCfg.Treasury,
		Vault:             vault,
	})
}

// groupDuePositions groups the positions to redeem now. The hourly worker waits for a position to
// pass the redeem threshold, be closing, or hold a voucher an hour old (fees are batched); with
// force (an operator's "redeem now") every position with something to claim is due.
func (r *Runtime) groupDuePositions(
	ctx context.Context,
	positions []Position,
	skip map[string]bool,
	now int64,
	force bool,
) (map[string]*redeemGroup, uint64) {
	groups := map[string]*redeemGroup{}

	var unredeemed uint64

	for i := range positions {
		position := positions[i]
		value := position.Cumulative - min(position.Cumulative, position.Redeemed)

		age := now - position.AcceptedUnix
		if position.Mode == dvpn.PositionModeStream {
			accrued := program.StreamAccrued(
				position.Escrow, position.StreamBase, position.RatePerHour, position.StreamStartedUnix, now,
			)
			value = accrued - min(accrued, position.Redeemed)
			age = now - position.RedeemedUnix
		}

		unredeemed += value

		due := (force && value > 0) || (position.Status == PositionStatusClosing && value > 0) ||
			value >= r.cfg.Redeem.ThresholdBaseUnits || (value > 0 && age >= int64(redeemInterval.Seconds()))
		if !due || skip[position.Address] {
			continue
		}

		address, _ := solana.PublicKeyFromBase58(position.Address)

		fresh, err := r.refreshPosition(ctx, position.Kind, address)
		if err != nil {
			r.log.Warn().Err(err).Str("position", position.Address).Msg("refreshing due position")

			continue
		}

		key := groupKey(fresh.Kind, fresh.Mode)

		group, ok := groups[key]
		if !ok {
			group = &redeemGroup{
				key:          key,
				kind:         batchKind(fresh.Mode),
				positionKind: fresh.Kind,
				mode:         fresh.Mode,
				subjects:     map[solana.PublicKey]redeemSubject{},
			}
			groups[key] = group
		}

		subject := redeemSubject{
			nodeOpenedUnix: fresh.NodeOpenedUnix,
			openedUnix:     fresh.OpenedUnix,
		}

		if fresh.Mode != dvpn.PositionModeVoucher {
			accrued := program.StreamAccrued(fresh.Escrow, fresh.StreamBase, fresh.RatePerHour, fresh.StreamStartedUnix, now)
			if accrued > fresh.Redeemed {
				group.subjects[address] = subject
			}

			continue
		}

		if len(fresh.Signature) == 0 || fresh.Cumulative <= fresh.Redeemed {
			continue
		}

		subject.voucher.CumulativeAmount = fresh.Cumulative
		copy(subject.voucher.Signature[:], fresh.Signature)
		group.subjects[address] = subject
	}

	return groups, unredeemed
}

func (r *Runtime) redeem(ctx context.Context) error { return r.redeemPositions(ctx, false) }

// redeemNow claims every position that has something to claim, below the threshold too.
func (r *Runtime) redeemNow(ctx context.Context) error { return r.redeemPositions(ctx, true) }

func (r *Runtime) redeemPositions(ctx context.Context, force bool) error {
	// One pass at a time. The hourly job, position sync and RedeemNow can overlap, and two passes over the same due
	// positions build the same transaction (same subjects, voucher and recent blockhash), so the second save fails
	// on the signature's unique key and leaves that batch due. A pass that waits here reads the advanced positions.
	r.redeemMu.Lock()
	defer r.redeemMu.Unlock()

	skip, err := r.resolvePendingRedemptions(ctx)
	if err != nil {
		return err
	}

	var positions []Position
	if err := r.db.WithContext(ctx).Where("status <> ?", PositionStatusClosed).Find(&positions).Error; err != nil {
		return fmt.Errorf("loading positions: %w", err)
	}

	groups, unredeemed := r.groupDuePositions(ctx, positions, skip, time.Now().Unix(), force)
	r.metrics.unredeemedBaseUnits.Set(float64(unredeemed))

	for _, group := range groups {
		r.redeemBatches(ctx, group)
	}

	if err := r.sweepFees(ctx); err != nil {
		r.log.Warn().Err(err).Msg("sweeping node fees")
	}

	return nil
}

func (r *Runtime) redeemBatch(ctx context.Context, group *redeemGroup, subjects []solana.PublicKey) error {
	instruction, err := r.buildRedeemInstruction(group, subjects)
	if err != nil {
		return err
	}

	units := program.ComputeUnits(group.kind, len(subjects))
	if r.sender.Paymaster != nil {
		units += paymaster.FeeTransferComputeUnits
	}

	receipt, sendErr := r.sender.Send(
		ctx, r.privateKey, []solana.Instruction{instruction}, tx.WithLabel(redeemLabel), tx.WithComputeUnitLimit(units),
	)
	now := time.Now().Unix()
	row := Redemption{
		Kind:     group.key,
		SentUnix: now,
		Status:   RedemptionStatusConfirmed,
		Subjects: formatSubjects(subjects),
	}
	result := resultOK

	if sendErr != nil {
		row.Error = sendErr.Error()
		result = resultError

		if indeterminate, ok := errors.AsType[*tx.IndeterminateError](sendErr); ok {
			row.Signature = indeterminate.Signature.String()
			row.Status = RedemptionStatusPending
		} else {
			row.Signature = fmt.Sprintf("failed-%d-%s-%s", now, subjects[0], uuid.NewString())
			row.Status = RedemptionStatusFailed
		}
	} else {
		row.ConfirmedUnix = now
		row.Signature = receipt.Signature.String()
	}

	mode := strconv.Itoa(int(group.mode))
	r.metrics.redemptions.WithLabelValues(group.positionKind, mode, result).Add(float64(len(subjects)))

	if err := r.db.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("saving redemption: %w", err)
	}

	if row.Status == RedemptionStatusPending {
		return nil
	}

	if sendErr != nil {
		return fmt.Errorf("redeeming batch: %w", sendErr)
	}

	for _, subject := range subjects {
		redeemed := group.subjects[subject]

		updates := map[string]any{"redeemed_unix": now}
		if redeemed.voucher.CumulativeAmount != 0 {
			updates["redeemed"] = redeemed.voucher.CumulativeAmount
		}

		if err := r.db.WithContext(ctx).Model(&Position{}).Where(map[string]any{
			"address":          subject.String(),
			"kind":             group.positionKind,
			"mode":             group.mode,
			"node_opened_unix": redeemed.nodeOpenedUnix,
			"opened_unix":      redeemed.openedUnix,
		}).Updates(updates).Error; err != nil {
			r.log.Warn().Err(err).Str("position", subject.String()).Msg("updating redeemed position")
		}
	}

	return nil
}

func (r *Runtime) redeemBatches(ctx context.Context, group *redeemGroup) {
	all := make([]solana.PublicKey, 0, len(group.subjects))
	for subject := range group.subjects {
		all = append(all, subject)
	}

	warn := func(subject solana.PublicKey, err error) {
		r.log.Warn().Err(err).Str("position", subject.String()).Msg("single redemption failed")
	}

	maxBytes := r.sender.Config.MaxTransactionBytes
	if r.sender.Paymaster != nil {
		maxBytes = min(maxBytes, solana.MaxTransactionSizeV1) - paymaster.FeeTransferOverheadBytes
	}

	if program.CapacityFor(group.kind, maxBytes) == 0 {
		r.log.Warn().Int("max_bytes", maxBytes).Str("group", group.key).Msg("no room for a redemption batch")
	}

	for _, batch := range program.SplitBatchesFor(group.kind, maxBytes, all) {
		err := r.redeemBatch(ctx, group, batch)
		if err == nil {
			continue
		}

		if len(batch) == 1 {
			warn(batch[0], err)

			continue
		}

		for _, subject := range batch {
			if err := r.redeemBatch(ctx, group, []solana.PublicKey{subject}); err != nil {
				warn(subject, err)
			}
		}
	}
}

func (r *Runtime) resolvePendingRedemptions(ctx context.Context) (map[string]bool, error) {
	var rows []Redemption
	if err := r.db.WithContext(ctx).Where("status = ?", RedemptionStatusPending).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("loading pending redemptions: %w", err)
	}

	skip := make(map[string]bool)
	if len(rows) == 0 {
		return skip, nil
	}

	signatures := make([]solana.Signature, 0, len(rows))
	for _, row := range rows {
		signature, err := solana.SignatureFromBase58(row.Signature)
		if err != nil {
			return nil, fmt.Errorf("parsing redemption signature: %w", err)
		}

		signatures = append(signatures, signature)
	}

	statuses, err := r.programClient.RPC.GetSignatureStatuses(ctx, true, signatures...)
	if err != nil {
		return nil, fmt.Errorf("checking pending redemptions: %w", err)
	}

	now := time.Now().Unix()

	for i := range rows {
		row := &rows[i]

		var status *rpc.SignatureStatusesResult
		if i < len(statuses.Value) {
			status = statuses.Value[i]
		}

		switch {
		case status != nil && status.Err != nil:
			row.Status = RedemptionStatusFailed
			row.Error = fmt.Sprint(status.Err)
		case status != nil && (status.ConfirmationStatus == rpc.ConfirmationStatusConfirmed ||
			status.ConfirmationStatus == rpc.ConfirmationStatusFinalized):
			row.Status = RedemptionStatusConfirmed
			row.ConfirmedUnix = now
		case status == nil && now-row.SentUnix >= int64(pendingRedemptionTTL.Seconds()):
			row.Status = RedemptionStatusFailed
			row.Error = "transaction not found"
		default:
			for _, subject := range parseSubjects(row.Subjects) {
				skip[subject] = true
			}

			continue
		}

		if err := r.db.WithContext(ctx).Save(row).Error; err != nil {
			return nil, fmt.Errorf("saving redemption: %w", err)
		}
	}

	return skip, nil
}

func (r *Runtime) sweepFees(ctx context.Context) error {
	if err := r.syncChain(ctx); err != nil {
		return err
	}

	chainCfg, node := r.chainState()
	if node.FeesAccrued == 0 || node.FeesAccrued < r.cfg.Redeem.ThresholdBaseUnits {
		return nil
	}

	instruction, err := r.buildSweepInstruction(chainCfg)
	if err != nil {
		return err
	}

	_, err = r.sender.Send(ctx, r.privateKey, []solana.Instruction{instruction}, tx.WithLabel("sweep_node_fees"))

	return err
}

func batchKind(mode uint8) program.BatchKind {
	if mode == dvpn.PositionModeStream {
		return program.BatchRedeemStreams
	}

	return program.BatchRedeemVouchers
}

func groupKey(kind string, mode uint8) string {
	return fmt.Sprintf("%s/%d", kind, mode)
}
