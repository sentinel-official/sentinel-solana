package client

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/solana-foundation/solana-go/v2"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-go-sdk/v3/voucher"
)

func (r *Runtime) blockFor(terms session.Terms, settlement nodeapi.Settlement) (uint64, error) {
	minimum := settlement.MinChannelPrepayBytes
	maximum := r.cfg.MaxPrepayBytes
	configured := r.cfg.PrepayBytes
	key := "max_prepay_bytes"

	if terms.Basis == session.BasisPerHour {
		minimum = settlement.MinChannelPrepaySeconds
		maximum = r.cfg.MaxPrepaySeconds
		configured = r.cfg.PrepaySeconds
		key = "max_prepay_seconds"
	}

	block, err := session.PrepayBlock(terms, configured, minimum, maximum)
	if errors.Is(err, session.ErrPrepayExceedsMaximum) {
		return 0, fmt.Errorf("node minimum %d is over client %s %d: %w", minimum, key, maximum, err)
	}

	return block, err //nolint:wrapcheck // the session errors name the cause
}

func (r *Runtime) settledBound(
	ctx context.Context,
	channelAddress string,
	blockBaseUnits uint64,
	percent uint8,
) (uint64, error) {
	var sessions []Session
	if err := r.db.WithContext(ctx).
		Where("channel_address = ? AND status = ?", channelAddress, SessionStatusEnded).
		Find(&sessions).Error; err != nil {
		return 0, fmt.Errorf("reading ended sessions: %w", err)
	}

	var bound uint64

	for _, ended := range sessions {
		amount := ended.PaidBaseUnits - min(ended.PaidBaseUnits, ended.SettledBaseUnits)
		if ended.Clean {
			amount = saturatingAdd(ended.OwedBaseUnits, session.Tolerance(ended.OwedBaseUnits, 0, percent))
			amount = saturatingAdd(amount, session.Tolerance(0, blockBaseUnits, percent))
		}

		bound = saturatingAdd(bound, amount)
	}

	return bound, nil
}

func (r *Runtime) verifyLedger(ch *Channel, view nodeapi.Position, terms session.Terms, settledBound uint64) error {
	if view.Ledger.Status != nodeapi.LedgerStatusOpen {
		return api.ErrChannelNotOpen
	}

	if view.Ledger.EscrowBaseUnits != ch.Escrow ||
		view.Ledger.RedeemedBaseUnits != ch.Redeemed || view.Ledger.OpenedAt != ch.OpenedUnix {
		return session.ErrLedgerMismatch
	}

	if view.Ledger.CumulativeBaseUnits > view.Ledger.RedeemedBaseUnits {
		address, err := solana.PublicKeyFromBase58(ch.Address)
		if err != nil {
			return fmt.Errorf("%w: parsing channel address: %w", session.ErrInvalidLedger, err)
		}

		message := voucher.ChannelMessage(address, ch.OpenedUnix, view.Ledger.CumulativeBaseUnits)
		if err := voucher.Verify(r.signer, message, view.Ledger.Signature); err != nil {
			return fmt.Errorf("%w: %w", session.ErrInvalidLedger, err)
		}
	}

	if view.Consumption.OwedBaseUnits > view.Ledger.CumulativeBaseUnits {
		return session.ErrOwedExceedsPaid
	}

	if view.Consumption.SettledBaseUnits > settledBound {
		return session.ErrSettledExceedsBound
	}

	if view.Ledger.Basis == terms.Basis && view.Ledger.Price != terms.Price {
		return session.ErrNodePriceChanged
	}

	return nil
}

func saturatingAdd(value, increment uint64) uint64 {
	if increment > math.MaxUint64-value {
		return math.MaxUint64
	}

	return value + increment
}
