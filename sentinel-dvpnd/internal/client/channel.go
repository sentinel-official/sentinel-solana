package client

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/preflight"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-go-sdk/v3/voucher"
	"github.com/sentinel-official/sentinel-programs/clients/go/core"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	pendingChannelInterval = 2 * time.Second
	pendingChannelTimeout  = 60 * time.Second
	pendingChannelTTL      = 5 * time.Minute
)

var (
	errChannelNotSignable = errors.New("the operator key is not the voucher signer of this channel")
	errChannelPending     = errors.New("channel opening is pending")
	errInsufficientEscrow = errors.New("channel escrow below node minimum: fund or close the channel")
)

func (r *Runtime) applyChainChannel(ch *Channel, chainChannel dvpn.Channel) error {
	if chainChannel.Mode == dvpn.PositionModeVoucher && r.signer != chainChannel.VoucherSigner {
		return errChannelNotSignable
	}

	ch.Escrow = chainChannel.Escrow
	ch.Mode = chainChannel.Mode
	ch.OpenedUnix = chainChannel.OpenedAt
	ch.Redeemed = chainChannel.Redeemed

	return nil
}

func (r *Runtime) buildOpenChannel(
	ctx context.Context,
	id [32]uint8,
	nodeAddress solana.PublicKey,
	node dvpn.Node,
	mode uint32,
	escrow uint64,
) (solana.Instruction, error) {
	signer := r.signer

	var terms dvpn.PositionTerms

	if mode == uint32(dvpn.PositionModeStream) {
		streamTerms := dvpn.PositionTermsStream{ExpectedRatePerHour: node.ChannelPricePerHour}
		terms = &streamTerms
	} else {
		voucherTerms := dvpn.PositionTermsVoucher{VoucherSigner: signer}
		terms = &voucherTerms
	}

	vault, err := r.programClient.VaultAddress()
	if err != nil {
		return nil, err
	}

	channelAddress, _, err := dvpn.FindChannelAddress(r.programClient.ProgramID, r.owner, id)
	if err != nil {
		return nil, fmt.Errorf("deriving channel address: %w", err)
	}

	grant, err := r.programClient.GrantAddress(ctx, r.owner, signer, dvpn.PermOpenChannel, nodeAddress, channelAddress)
	if err != nil {
		return nil, err
	}

	ownerToken, err := r.programClient.AssociatedTokenAddress(r.owner)
	if err != nil {
		return nil, err
	}

	rentPayer, err := r.sender.RentPayer(ctx, signer)
	if err != nil {
		return nil, err
	}

	params := dvpn.OpenChannelParams{
		ChannelID:            id,
		ChannelRentRecipient: signer,
		Escrow:               escrow,
		Grant:                grant,
		Mint:                 r.programClient.Mint,
		Node:                 nodeAddress,
		Owner:                r.owner,
		OwnerToken:           ownerToken,
		RentPayer:            rentPayer,
		Signer:               signer,
		Terms:                terms,
		UserRentRecipient:    signer,
		Vault:                vault,
	}

	return dvpn.BuildOpenChannelInstruction(r.programClient.ProgramID, params)
}

func (r *Runtime) closeChannel(ctx context.Context, address solana.PublicKey, chainChannel dvpn.Channel) error {
	user, _, err := dvpn.FindUserAddress(r.programClient.ProgramID, chainChannel.Owner)
	if err != nil {
		return err
	}

	params := dvpn.CloseChannelParams{
		Channel:       address,
		RentRecipient: chainChannel.RentRecipient,
		Signer:        r.signer,
		User:          user,
	}

	ix, err := dvpn.BuildCloseChannelInstruction(r.programClient.ProgramID, params)
	if err != nil {
		return err
	}

	_, err = r.sender.Send(ctx, r.privateKey, []solana.Instruction{ix}, tx.WithLabel("close_channel"))

	return err
}

func (r *Runtime) closeChannels(ctx context.Context) error {
	if err := r.resolvePendingChannels(ctx); err != nil {
		return err
	}

	var channels []Channel
	if err := r.db.WithContext(ctx).Where("status IN ?", []string{
		ChannelStatusOpen, ChannelStatusClosing,
	}).Find(&channels).Error; err != nil {
		return err
	}

	configAccount, err := r.programClient.FetchConfigBySeeds(ctx)
	if err != nil {
		return fmt.Errorf("fetching program config: fetching config: %w", err)
	}

	now := time.Now().Unix()

	validChannels := make([]Channel, 0, len(channels))

	addresses := make([]solana.PublicKey, 0, len(channels))
	for _, ch := range channels {
		address, err := solana.PublicKeyFromBase58(ch.Address)
		if err != nil {
			r.log.Warn().Err(err).Str("channel", ch.Address).Msg("parsing channel address")

			continue
		}

		validChannels = append(validChannels, ch)
		addresses = append(addresses, address)
	}

	chainAccounts, batchErr := r.programClient.FetchChannelBatch(ctx, addresses)
	if batchErr != nil {
		r.log.Warn().Err(batchErr).Msg("fetching channels in a batch failed; fetching them one by one")
	}

	for i := range validChannels {
		ch := &validChannels[i]
		address := addresses[i]

		var chainChannel dvpn.Channel
		if batchErr == nil {
			chainChannel = chainAccounts[i].Value
		} else {
			account, err := r.programClient.FetchChannel(ctx, address)
			if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
				r.updateChannelStatus(ctx, ch, ChannelStatusClosed)

				continue
			}

			if err != nil {
				r.log.Warn().Err(err).Str("channel", ch.Address).Msg("fetching channel failed")

				continue
			}

			chainChannel = account.Value
		}

		if chainChannel.Status != dvpn.PositionStatusClosing {
			r.updateChannelStatus(ctx, ch, ChannelStatusOpen)

			continue
		}

		if now < chainChannel.CloseStartedAt+int64(configAccount.Value.ChannelCloseGraceSeconds) {
			r.updateChannelStatus(ctx, ch, ChannelStatusClosing)

			continue
		}

		if err := r.closeChannel(ctx, address, chainChannel); err != nil {
			r.log.Warn().Err(err).Str("channel", ch.Address).Msg("close_channel failed")

			continue
		}

		r.updateChannelStatus(ctx, ch, ChannelStatusClosed)
	}

	return nil
}

func (r *Runtime) openChannel(
	ctx context.Context,
	nodeAddress solana.PublicKey,
	node dvpn.Node,
	mode uint32,
	escrow uint64,
) (*Channel, error) {
	var pending Channel

	err := r.findFirst(ctx, &pending, "node_address = ? AND status = ?", nodeAddress.String(), ChannelStatusPending)
	if err == nil {
		if resolveErr := r.resolvePendingChannel(ctx, &pending); resolveErr != nil {
			r.log.Warn().Err(resolveErr).Str("channel", pending.Address).Msg("resolving pending channel")
		}

		err = r.findFirst(
			ctx, &pending, "address = ? AND status = ?", pending.Address, ChannelStatusPending,
		)
		if err == nil {
			return nil, fmt.Errorf("%w: %s", errChannelPending, pending.Address)
		}
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	var id [32]uint8
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("generating channel id: %w", err)
	}

	ix, err := r.buildOpenChannel(ctx, id, nodeAddress, node, mode, escrow)
	if err != nil {
		return nil, err
	}

	address, _, err := dvpn.FindChannelAddress(r.programClient.ProgramID, r.owner, id)
	if err != nil {
		return nil, fmt.Errorf("deriving channel address: %w", err)
	}

	ch := &Channel{
		Address:     address.String(),
		NodeAddress: nodeAddress.String(),
		Status:      ChannelStatusPending,
		UpdatedUnix: time.Now().Unix(),
	}
	if err := r.save(ctx, ch); err != nil {
		return nil, err
	}

	if _, err := r.sender.Send(ctx, r.privateKey, []solana.Instruction{ix}, tx.WithLabel("open_channel")); err != nil {
		err = r.explainOpenFailure(ctx, err, escrow)

		if isDefinitiveOpenFailure(err) {
			cleanupCtx, cancel := r.withDetachedTimeout(ctx)
			defer cancel()

			if deleteErr := r.db.WithContext(cleanupCtx).Delete(ch).Error; deleteErr != nil {
				return nil, errors.Join(err, deleteErr)
			}

			return nil, err
		}

		r.waitPendingChannel(ctx, ch)

		return nil, err
	}

	if err := r.resolvePendingChannel(ctx, ch); err != nil {
		return nil, err
	}

	if ch.Status == ChannelStatusPending {
		return nil, fmt.Errorf("%w: %s", errChannelPending, ch.Address)
	}

	return ch, nil
}

func (r *Runtime) resolveChannel(
	ctx context.Context,
	req *daemonv1.StartSessionRequest,
	nodeAddress solana.PublicKey,
	node dvpn.Node,
) (*Channel, error) {
	if nc := req.GetCredential().GetNewChannel(); nc != nil {
		escrow := nc.GetEscrow()
		if escrow == 0 {
			escrow = r.cfg.EscrowBaseUnits
		}

		return r.openChannel(ctx, nodeAddress, node, nc.GetMode(), escrow)
	}

	address, err := solana.PublicKeyFromBase58(req.GetCredential().GetChannel())
	if err != nil {
		return nil, fmt.Errorf("channel: %w", err)
	}

	channelAccount, err := r.programClient.FetchChannel(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("fetching channel %s: %w", address, err)
	}

	chainChannel := channelAccount.Value

	if chainChannel.Node != nodeAddress || chainChannel.Status != dvpn.PositionStatusOpen {
		return nil, api.ErrChannelNotOpen
	}

	ch := &Channel{
		Address:     address.String(),
		NodeAddress: nodeAddress.String(),
	}
	if err := r.findFirst(
		ctx, ch, "address = ?", address.String(),
	); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		r.log.Warn().Err(err).Msg("loading a stored channel")
	}

	if err := r.applyChainChannel(ch, chainChannel); err != nil {
		return nil, err
	}

	ch.Status = ChannelStatusOpen
	ch.UpdatedUnix = time.Now().Unix()

	return ch, r.save(ctx, ch)
}

func (r *Runtime) resolvePendingChannel(ctx context.Context, ch *Channel) error {
	address, err := solana.PublicKeyFromBase58(ch.Address)
	if err != nil {
		return fmt.Errorf("parsing pending channel: %w", err)
	}

	account, err := r.programClient.FetchChannel(ctx, address)
	if errors.Is(err, core.ErrAccountNotFound) {
		if time.Since(time.Unix(ch.UpdatedUnix, 0)) < pendingChannelTTL {
			return nil
		}

		return r.db.WithContext(ctx).
			Where("address = ? AND status = ?", ch.Address, ChannelStatusPending).
			Delete(&Channel{}).Error
	}

	if err != nil {
		return err
	}

	if account.Value.Owner != r.owner || account.Value.Node.String() != ch.NodeAddress {
		return fmt.Errorf("pending channel %s has different owner or node", ch.Address)
	}

	if err := r.applyChainChannel(ch, account.Value); err != nil {
		return err
	}

	status := ChannelStatusOpen
	if account.Value.Status == dvpn.PositionStatusClosing {
		status = ChannelStatusClosing
	}

	result := r.db.WithContext(ctx).Model(&Channel{}).
		Where("address = ? AND status = ?", ch.Address, ChannelStatusPending).
		Updates(map[string]any{
			"escrow":       ch.Escrow,
			"mode":         ch.Mode,
			"opened_unix":  ch.OpenedUnix,
			"redeemed":     ch.Redeemed,
			"status":       status,
			"updated_unix": time.Now().Unix(),
		})
	if result.Error != nil {
		return result.Error
	}

	return r.findFirst(ctx, ch, "address = ?", ch.Address)
}

func (r *Runtime) resolvePendingChannels(ctx context.Context) error {
	var channels []Channel
	if err := r.db.WithContext(ctx).Where("status = ?", ChannelStatusPending).Find(&channels).Error; err != nil {
		return err
	}

	for i := range channels {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("resolving pending channels: %w", err)
		}

		if err := r.resolvePendingChannel(ctx, &channels[i]); err != nil {
			r.log.Warn().Err(err).Str("channel", channels[i].Address).Msg("resolving pending channel")
		}
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("resolving pending channels: %w", err)
	}

	return nil
}

func (r *Runtime) signVoucher(ch *Channel, cumulative uint64) nodeapi.Voucher {
	address, _ := solana.PublicKeyFromBase58(ch.Address)
	signed := voucher.Sign(r.privateKey, voucher.ChannelMessage(address, ch.OpenedUnix, cumulative))

	return nodeapi.Voucher{
		Cumulative: signed.CumulativeAmount,
		Signature:  signed.Signature[:],
	}
}

func (r *Runtime) startCloseChannel(ctx context.Context, ch *Channel) error {
	address, _ := solana.PublicKeyFromBase58(ch.Address)

	signer := r.signer

	node, err := solana.PublicKeyFromBase58(ch.NodeAddress)
	if err != nil {
		return fmt.Errorf("parsing channel node: %w", err)
	}

	grant, err := r.programClient.GrantAddress(ctx, r.owner, signer, dvpn.PermStartCloseChannel, address, node)
	if err != nil {
		return err
	}

	params := dvpn.StartCloseChannelParams{
		Channel: address,
		Grant:   grant,
		Signer:  signer,
	}

	ix, err := dvpn.BuildStartCloseChannelInstruction(r.programClient.ProgramID, params)
	if err != nil {
		return err
	}

	instructions := []solana.Instruction{ix}
	if _, err := r.sender.Send(ctx, r.privateKey, instructions, tx.WithLabel("start_close_channel")); err != nil {
		return err
	}

	ch.Status = ChannelStatusClosing

	return r.save(ctx, ch)
}

func (r *Runtime) updateChannelStatus(ctx context.Context, ch *Channel, status string) {
	if status == ch.Status {
		return
	}

	if err := r.db.WithContext(ctx).Model(&Channel{}).
		Where("address = ? AND status = ?", ch.Address, ch.Status).
		Update("status", status).Error; err != nil {
		r.log.Warn().Err(err).Str("channel", ch.Address).Msg("recording channel status")
	}
}

func (r *Runtime) waitPendingChannel(ctx context.Context, ch *Channel) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.pendingChannelTimeout)
	defer cancel()

	ticker := time.NewTicker(r.pendingChannelInterval)
	defer ticker.Stop()

	for {
		if err := r.resolvePendingChannel(ctx, ch); err != nil {
			r.log.Warn().Err(err).Str("channel", ch.Address).Msg("resolving pending channel")
		}

		if ch.Status != ChannelStatusPending {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func isDefinitiveOpenFailure(err error) bool {
	if _, ok := errors.AsType[*tx.UnsentError](err); ok {
		return true
	}

	failed, ok := errors.AsType[*tx.FailedOnChainError](err)

	return ok && !failed.PriorIndeterminate
}

// explainOpenFailure adds the fix to a Token-2022 refusal of open_channel. The program moves the
// escrow as the owner's delegate, so a missing or used-up approval fails exactly like an empty wallet
// ("insufficient funds"), and a revoked one as "owner does not match" (the config is no longer the
// delegate); the raw error named neither.
func (r *Runtime) explainOpenFailure(ctx context.Context, err error, escrow uint64) error {
	txErr, ok := errors.AsType[*core.TxError](err)
	if !ok || txErr.Program != "token-2022" || (txErr.Name != "InsufficientFunds" && txErr.Name != "OwnerMismatch") {
		return err
	}

	readCtx, cancel := r.withDetachedTimeout(ctx)
	defer cancel()

	if problem := preflight.DelegateProblem(readCtx, r.programClient, r.owner); problem != "" {
		return fmt.Errorf("%w: the %s; %s with the client owner key", err, problem, preflight.ApproveDelegateRemedy)
	}

	return fmt.Errorf("%w: the delegate is approved, so the wallet balance or allowance is below the escrow of %d base units", err, escrow)
}
