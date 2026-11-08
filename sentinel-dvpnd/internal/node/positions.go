//go:build linux

package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/sentinel-official/sentinel-dvpnd/internal/chainread"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-programs/clients/go/core"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

func (r *Runtime) closeSessionsOfClosingPosition(
	ctx context.Context,
	position *Position,
	chainCfg dvpn.Config,
	now int64,
) {
	reason := nodeapi.ReasonPositionClosing

	if position.Kind == KindLease {
		if now < position.CloseStartedUnix+int64(chainCfg.LeaseCloseGraceSeconds) {
			return
		}

		reason = nodeapi.ReasonCloseGrace
	}

	r.closeSubjectSessions(ctx, position.Address, reason)
}

func (r *Runtime) closeSubjectSessions(ctx context.Context, subject, reason string) {
	var sessions []Session
	if err := r.db.WithContext(ctx).
		Where("subject = ? AND status = ?", subject, SessionStatusActive).Find(&sessions).Error; err != nil {
		r.log.Warn().Err(err).Str("subject", subject).Msg("loading subject sessions")

		return
	}

	for i := range sessions {
		if err := r.closeSession(ctx, &sessions[i], reason); err != nil {
			r.log.Warn().Err(err).Str("session", sessions[i].ID).Msg("closing subject session")
		}
	}
}

func (r *Runtime) markPositionClosed(ctx context.Context, position *Position, now int64) (bool, error) {
	result := r.db.WithContext(ctx).Model(&Position{}).Where(map[string]any{
		"address":          position.Address,
		"chain_slot":       position.ChainSlot,
		"kind":             position.Kind,
		"mode":             position.Mode,
		"node_opened_unix": position.NodeOpenedUnix,
		"opened_unix":      position.OpenedUnix,
	}).Updates(map[string]any{
		"status":       PositionStatusClosed,
		"updated_unix": gorm.Expr("MAX(updated_unix, ?)", now),
	})
	if result.Error != nil {
		return false, fmt.Errorf("closing position: %w", result.Error)
	}

	return result.RowsAffected != 0, nil
}

func (r *Runtime) refreshPosition(ctx context.Context, kind string, address solana.PublicKey) (*Position, error) {
	switch kind {
	case KindChannel:
		account, err := r.programClient.FetchChannel(ctx, address)

		return r.refreshedChannel(ctx, address, account, err)
	case KindLease:
		account, err := r.programClient.FetchLease(ctx, address)

		return r.refreshedLease(ctx, address, account, err)
	default:
		return nil, api.ErrBadRequest
	}
}

// refreshedChannel saves a channel read from the chain (one fetch or one item of a batch).
func (r *Runtime) refreshedChannel(
	ctx context.Context,
	address solana.PublicKey,
	account core.Keyed[dvpn.Channel],
	err error,
) (*Position, error) {
	if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
		return nil, api.ErrChannelNotOpen
	}

	if err != nil {
		return nil, fmt.Errorf("fetching channel %s: %w", address, err)
	}

	if account.Value.Node != r.address {
		return nil, api.ErrChannelNotOpen
	}

	position := positionFromChannel(address, account.Value)
	position.ChainSlot = account.Slot

	return r.saveFetchedPosition(ctx, position)
}

// refreshedLease saves a lease read from the chain (one fetch or one item of a batch).
func (r *Runtime) refreshedLease(
	ctx context.Context,
	address solana.PublicKey,
	account core.Keyed[dvpn.Lease],
	err error,
) (*Position, error) {
	if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
		return nil, api.ErrChannelNotOpen
	}

	if err != nil {
		return nil, fmt.Errorf("fetching lease %s: %w", address, err)
	}

	if account.Value.Node != r.address {
		return nil, api.ErrChannelNotOpen
	}

	position := positionFromLease(address, account.Value)
	position.ChainSlot = account.Slot

	return r.saveFetchedPosition(ctx, position)
}

// fetchPositions reads every position's account in batches and returns, per index, the same
// result refreshPosition would give for it.
func (r *Runtime) fetchPositions(
	ctx context.Context,
	positions []Position,
) (func(context.Context, int) (*Position, error), error) {
	var channelAddrs, leaseAddrs []solana.PublicKey

	addresses := make([]solana.PublicKey, len(positions))
	slot := make([]int, len(positions))

	for i := range positions {
		addresses[i], _ = solana.PublicKeyFromBase58(positions[i].Address)

		switch positions[i].Kind {
		case KindChannel:
			slot[i] = len(channelAddrs)
			channelAddrs = append(channelAddrs, addresses[i])
		case KindLease:
			slot[i] = len(leaseAddrs)
			leaseAddrs = append(leaseAddrs, addresses[i])
		}
	}

	channels, err := chainread.Channels(ctx, &r.programClient.Client, channelAddrs)
	if err != nil {
		return nil, fmt.Errorf("fetching %d channels: %w", len(channelAddrs), err)
	}

	leases, err := chainread.Leases(ctx, &r.programClient.Client, leaseAddrs)
	if err != nil {
		return nil, fmt.Errorf("fetching %d leases: %w", len(leaseAddrs), err)
	}

	return func(ctx context.Context, i int) (*Position, error) {
		switch positions[i].Kind {
		case KindChannel:
			got := channels[slot[i]]

			return r.refreshedChannel(ctx, addresses[i], got.Account, got.Err)
		case KindLease:
			got := leases[slot[i]]

			return r.refreshedLease(ctx, addresses[i], got.Account, got.Err)
		default:
			return nil, api.ErrBadRequest
		}
	}, nil
}

// saveFetchedPosition stores a position read from the chain (one fetch or one item of a batch).
func (r *Runtime) saveFetchedPosition(ctx context.Context, position *Position) (*Position, error) {
	if position.Kind == KindLease && position.Mode == dvpn.PositionModeVoucher && position.RatePerHour == 0 {
		_, node := r.chainState()
		position.RatePerHour = node.LeasePricePerHour
	}

	position.UpdatedUnix = time.Now().Unix()

	return r.saveRefreshedPosition(ctx, position)
}

func (r *Runtime) saveRefreshedPosition(ctx context.Context, fresh *Position) (*Position, error) {
	for {
		position := *fresh

		var stored Position

		err := r.db.WithContext(ctx).First(&stored, "address = ?", position.Address).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			result := r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&position)
			if result.Error != nil {
				return nil, fmt.Errorf("creating position: %w", result.Error)
			}

			if result.RowsAffected != 0 {
				return &position, nil
			}

			continue
		}

		if err != nil {
			return nil, fmt.Errorf("loading position: %w", err)
		}

		if stored.ChainSlot > position.ChainSlot {
			return &stored, nil
		}

		if samePosition(&position, &stored) {
			mergePosition(&position, &stored)
		}

		result := r.db.WithContext(ctx).Model(&Position{}).Where(map[string]any{
			"accepted_unix":    stored.AcceptedUnix,
			"address":          stored.Address,
			"chain_slot":       stored.ChainSlot,
			"cumulative":       stored.Cumulative,
			"kind":             stored.Kind,
			"mode":             stored.Mode,
			"node_opened_unix": stored.NodeOpenedUnix,
			"opened_unix":      stored.OpenedUnix,
			"redeemed":         stored.Redeemed,
			"redeemed_unix":    stored.RedeemedUnix,
			"settled":          stored.Settled,
			"status":           stored.Status,
			"updated_unix":     stored.UpdatedUnix,
		}).Updates(map[string]any{
			"accepted_unix":       position.AcceptedUnix,
			"chain_slot":          position.ChainSlot,
			"close_started_unix":  position.CloseStartedUnix,
			"cumulative":          position.Cumulative,
			"escrow":              position.Escrow,
			"kind":                position.Kind,
			"mode":                position.Mode,
			"node_opened_unix":    position.NodeOpenedUnix,
			"opened_unix":         position.OpenedUnix,
			"opener":              position.Opener,
			"owner":               position.Owner,
			"provider":            position.Provider,
			"rate_per_hour":       position.RatePerHour,
			"redeemed":            position.Redeemed,
			"redeemed_unix":       position.RedeemedUnix,
			"settled":             position.Settled,
			"signature":           position.Signature,
			"status":              position.Status,
			"stream_base":         position.StreamBase,
			"stream_started_unix": position.StreamStartedUnix,
			"updated_unix":        position.UpdatedUnix,
			"voucher_signer":      position.VoucherSigner,
		})
		if result.Error != nil {
			return nil, fmt.Errorf("saving position: %w", result.Error)
		}

		if result.RowsAffected != 0 {
			if position.Kind == KindLease && stored.Status == PositionStatusOpen &&
				position.Status == PositionStatusClosing && samePosition(&position, &stored) {
				if err := r.publishClosed(position.Address, nodeapi.ReasonPositionClosing); err != nil {
					r.log.Warn().Err(err).Str("lease", position.Address).Msg("publishing closing lease")
				}
			}

			return &position, nil
		}
	}
}

func (r *Runtime) syncPositions(ctx context.Context) error {
	var positions []Position
	if err := r.db.WithContext(ctx).Where("status <> ?", PositionStatusClosed).Find(&positions).Error; err != nil {
		return fmt.Errorf("loading positions: %w", err)
	}

	chainCfg, _ := r.chainState()
	now := time.Now().Unix()
	redeemClosing := false

	// Runs every minute over every open position: one getMultipleAccounts per 100 channels and
	// per 100 leases, instead of one getAccountInfo per position.
	fetched, err := r.fetchPositions(ctx, positions)
	if err != nil {
		return err
	}

	for i := range positions {
		stored := positions[i]

		fresh, err := fetched(ctx, i)
		if errors.Is(err, api.ErrChannelNotOpen) {
			closed, closeErr := r.markPositionClosed(ctx, &stored, now)
			if closeErr != nil {
				r.log.Warn().Err(closeErr).Str("position", stored.Address).Msg("saving closed position")
			} else if closed {
				if stored.Kind == KindLease {
					if err := r.publishClosed(stored.Address, nodeapi.ReasonPositionClosed); err != nil {
						r.log.Warn().Err(err).Str("lease", stored.Address).Msg("publishing closed lease")
					}
				}

				r.closeSubjectSessions(ctx, stored.Address, nodeapi.ReasonPositionClosed)
			}

			continue
		}

		if err != nil {
			return err
		}

		if stored.Status != PositionStatusClosing && fresh.Status == PositionStatusClosing {
			redeemClosing = true
		}

		if fresh.Status == PositionStatusClosing {
			r.closeSessionsOfClosingPosition(ctx, fresh, chainCfg, now)
		}

		if fresh.Mode == dvpn.PositionModeStream {
			r.updateStreamEnforcer(ctx, fresh)
		}
	}

	if redeemClosing {
		if err := r.redeem(ctx); err != nil {
			r.log.Warn().Err(err).Msg("redeeming positions entering closing")
		}
	}

	return nil
}

func (r *Runtime) updateStreamEnforcer(ctx context.Context, position *Position) {
	var sess Session
	if err := r.db.WithContext(ctx).
		Where("subject = ? AND status = ?", position.Address, SessionStatusActive).First(&sess).Error; err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if enforcer, ok := r.enforcers[sess.ID].(*session.StreamEnforcer); ok {
		enforcer.Update(position.Escrow, position.StreamBase, position.StreamStartedUnix)
	}
}

func formatPositionStatus(status uint8) string {
	if status == dvpn.PositionStatusClosing {
		return PositionStatusClosing
	}

	return PositionStatusOpen
}

func mergePosition(position, stored *Position) {
	position.AcceptedUnix = stored.AcceptedUnix
	position.Redeemed = max(position.Redeemed, stored.Redeemed)
	position.Cumulative = max(position.Cumulative, stored.Cumulative, position.Redeemed)
	position.RedeemedUnix = stored.RedeemedUnix
	position.Settled = stored.Settled
	position.UpdatedUnix = max(position.UpdatedUnix, stored.UpdatedUnix)

	if position.Cumulative == stored.Cumulative {
		position.Signature = stored.Signature
	} else {
		position.AcceptedUnix = 0
	}

	if stored.Kind == KindLease || stored.RatePerHour != 0 {
		position.RatePerHour = stored.RatePerHour
	}

	if stored.Status == PositionStatusClosing || stored.Status == PositionStatusClosed {
		position.Status = stored.Status
		position.CloseStartedUnix = max(position.CloseStartedUnix, stored.CloseStartedUnix)
	}
}

func positionFromChannel(address solana.PublicKey, channel dvpn.Channel) *Position {
	return &Position{
		Address:           address.String(),
		CloseStartedUnix:  channel.CloseStartedAt,
		Cumulative:        channel.Redeemed,
		Escrow:            channel.Escrow,
		Kind:              KindChannel,
		Mode:              channel.Mode,
		NodeOpenedUnix:    channel.NodeOpenedAt,
		OpenedUnix:        channel.OpenedAt,
		Opener:            channel.Opener.String(),
		Owner:             channel.Owner.String(),
		RatePerHour:       channel.RatePerHour,
		Redeemed:          channel.Redeemed,
		Status:            formatPositionStatus(channel.Status),
		StreamBase:        channel.StreamBase,
		StreamStartedUnix: channel.StreamStartedAt,
		VoucherSigner:     channel.VoucherSigner.String(),
	}
}

func positionFromLease(address solana.PublicKey, lease dvpn.Lease) *Position {
	return &Position{
		Address:           address.String(),
		CloseStartedUnix:  lease.CloseStartedAt,
		Cumulative:        lease.Redeemed,
		Escrow:            lease.Escrow,
		Kind:              KindLease,
		Mode:              lease.Mode,
		NodeOpenedUnix:    lease.NodeOpenedAt,
		OpenedUnix:        lease.OpenedAt,
		Opener:            lease.Opener.String(),
		Owner:             lease.Owner.String(),
		Provider:          lease.Provider.String(),
		RatePerHour:       lease.RatePerHour,
		Redeemed:          lease.Redeemed,
		Status:            formatPositionStatus(lease.Status),
		StreamBase:        lease.StreamBase,
		StreamStartedUnix: lease.StreamStartedAt,
		VoucherSigner:     lease.VoucherSigner.String(),
	}
}

func samePosition(position, stored *Position) bool {
	return position.OpenedUnix == stored.OpenedUnix && position.Kind == stored.Kind &&
		position.Mode == stored.Mode && position.NodeOpenedUnix == stored.NodeOpenedUnix
}
