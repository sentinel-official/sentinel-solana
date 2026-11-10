//go:build linux

package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc/jsonrpc"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

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

// chainRetryDelays: the waits before each retry of an API-path chain read that hit a rate limit or
// a server error.
//
//nolint:gochecknoglobals // a var so tests can change it
var chainRetryDelays = []time.Duration{250 * time.Millisecond, 750 * time.Millisecond}

// positionForRequest is refreshPosition for the API paths (views, voucher admission, session start).
// A read that fails only because the RPC is rate limiting the node or failing is retried after a
// short wait; if it still fails the request answers api.ErrChainBusy (a 429 the client's transport
// retries, with a plain reason) instead of a 500 "internal error".
func (r *Runtime) positionForRequest(ctx context.Context, kind string, address solana.PublicKey) (*Position, error) {
	position, err := r.refreshPosition(ctx, kind, address)

	for _, delay := range chainRetryDelays {
		if err == nil || !chainUnavailable(err) {
			return position, err
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %w", api.ErrChainBusy, err)
		case <-time.After(delay):
		}

		position, err = r.refreshPosition(ctx, kind, address)
	}

	if err == nil || !chainUnavailable(err) {
		return position, err
	}

	r.log.Warn().Err(err).Str("position", address.String()).Str("kind", kind).
		Msg("chain read still unavailable after retries; answering busy")

	return nil, fmt.Errorf("%w: %w", api.ErrChainBusy, err)
}

// rpcRateLimitCode is the JSON-RPC error code that RPC providers and gateways answer a rate limit
// with inside an HTTP 200 reply (it is not the HTTP 429 a public RPC sends).
const rpcRateLimitCode = -32429

// chainUnavailable: a Solana RPC rate limit or server error (JSON-RPC or HTTP 429/5xx, or JSON-RPC
// -32429).
func chainUnavailable(err error) bool {
	if rpcErr, ok := errors.AsType[*jsonrpc.RPCError](err); ok {
		return rpcErr.Code == http.StatusTooManyRequests || rpcErr.Code == rpcRateLimitCode ||
			rpcErr.Code >= http.StatusInternalServerError
	}

	if httpErr, ok := errors.AsType[*jsonrpc.HTTPError](err); ok {
		return httpErr.Code == http.StatusTooManyRequests || httpErr.Code >= http.StatusInternalServerError
	}

	return strings.Contains(strings.ToLower(err.Error()), "too many requests")
}

func (r *Runtime) refreshPosition(ctx context.Context, kind string, address solana.PublicKey) (*Position, error) {
	var position *Position

	switch kind {
	case KindChannel:
		account, err := r.programClient.FetchChannel(ctx, address)
		if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
			return nil, api.ErrChannelNotOpen
		}

		if err != nil {
			return nil, fmt.Errorf("fetching channel %s: %w", address, err)
		}

		if account.Value.Node != r.address {
			return nil, api.ErrChannelNotOpen
		}

		position = positionFromChannel(address, account.Value)
		position.ChainSlot = account.Slot
	case KindLease:
		account, err := r.programClient.FetchLease(ctx, address)
		if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
			return nil, api.ErrChannelNotOpen
		}

		if err != nil {
			return nil, fmt.Errorf("fetching lease %s: %w", address, err)
		}

		if account.Value.Node != r.address {
			return nil, api.ErrChannelNotOpen
		}

		position = positionFromLease(address, account.Value)
		position.ChainSlot = account.Slot
	default:
		return nil, api.ErrBadRequest
	}

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

	for i := range positions {
		stored := positions[i]
		address, _ := solana.PublicKeyFromBase58(stored.Address)

		fresh, err := r.refreshPosition(ctx, stored.Kind, address)
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
