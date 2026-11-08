//go:build linux

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	directionRx         = "rx"
	directionTx         = "tx"
	idleNoiseFloorBytes = 512
)

func (r *Runtime) closeReason(sess *Session, usage session.Usage, now int64) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	enforcer, ok := r.enforcers[sess.ID]
	if ticket, isTicket := enforcer.(*session.TicketEnforcer); isTicket {
		switch {
		case ticket.LeaseUnhealthy():
			return nodeapi.ReasonLeaseNotFunded
		case ticket.Expired(now):
			return nodeapi.ReasonTicketExpired
		case ticket.CapReached(usage):
			return nodeapi.ReasonDataCapReached
		}
	}

	switch {
	case !ok:
		return nodeapi.ReasonNoEnforcer
	case enforcer.Exhausted(usage, now):
		return nodeapi.ReasonCreditExhausted
	case now-sess.LastTrafficUnix > int64(r.cfg.Sessions.IdleSeconds):
		return nodeapi.ReasonIdle
	default:
		return ""
	}
}

func (r *Runtime) requestChannelVoucher(ctx context.Context, sess *Session) error {
	if sess.Credential != CredentialChannel || sess.Price == 0 {
		return nil
	}

	basis := session.Basis(sess.Basis)
	minimum := session.Owed(basis, sess.Price, r.minPrepayFor(basis)) / 2
	r.mu.Lock()
	meter, ok := r.enforcers[sess.ID].(*session.Meter)
	request := ok && meter.Headroom(sess.Usage()) <= minimum
	r.mu.Unlock()

	if !request {
		return nil
	}

	var position Position
	if err := r.db.WithContext(ctx).First(&position, "address = ?", sess.Subject).Error; err != nil {
		return fmt.Errorf("loading channel position: %w", err)
	}

	if position.Kind != KindChannel || position.Mode != dvpn.PositionModeVoucher {
		return nil
	}

	return r.requestVoucher(ctx, &position, sess.ID)
}

func (r *Runtime) requestLeaseVouchers(ctx context.Context, now int64) {
	r.mu.Lock()
	for topic, state := range r.voucherRequests {
		if now-state.sentUnix >= int64(r.cfg.Sessions.HeartbeatSeconds) {
			delete(r.voucherRequests, topic)
		}
	}
	r.mu.Unlock()

	var positions []Position
	if err := r.db.WithContext(ctx).
		Where("kind = ? AND mode = ? AND status = ?", KindLease, dvpn.PositionModeVoucher, PositionStatusOpen).
		Where("rate_per_hour > 0").
		Find(&positions).Error; err != nil {
		r.log.Warn().Err(err).Msg("loading voucher leases")

		return
	}

	for i := range positions {
		position := &positions[i]
		elapsed := uint64(max(now-position.OpenedUnix, 0))
		owed := session.Owed(session.BasisPerHour, position.RatePerHour, elapsed)

		minimum := session.Owed(session.BasisPerHour, position.RatePerHour, r.cfg.Lease.MinPrepaySeconds) / 2
		if session.Headroom(position.Cumulative, owed) <= minimum {
			if err := r.requestVoucher(ctx, position, position.Address); err != nil {
				r.log.Warn().Err(err).Str("lease", position.Address).Msg("requesting lease voucher")
			}
		}
	}
}

func (r *Runtime) requestVoucher(ctx context.Context, position *Position, topic string) error {
	view, err := r.positionView(ctx, position)
	if err != nil {
		return err
	}

	data, err := json.Marshal(nodeapi.VoucherRequestEvent(view))
	if err != nil {
		return fmt.Errorf("marshaling voucher request: %w", err)
	}

	now := time.Now().Unix()

	r.mu.Lock()
	state, ok := r.voucherRequests[topic]

	if ok && state.cumulativeBaseUnits == position.Cumulative &&
		now-state.sentUnix < int64(r.cfg.Sessions.HeartbeatSeconds) {
		r.mu.Unlock()

		return nil
	}

	r.voucherRequests[topic] = voucherRequestState{
		cumulativeBaseUnits: position.Cumulative,
		sentUnix:            now,
	}
	r.mu.Unlock()

	r.events.publish(topic, api.EncodeEvent(api.Event{
		Data: data,
		Name: nodeapi.EventVoucherRequest,
	}))
	r.log.Debug().Str("kind", position.Kind).Str("position", position.Address).Msg("requested voucher")
	r.metrics.voucherRequests.WithLabelValues(position.Kind).Inc()

	return nil
}

func (r *Runtime) retryClosingSessions(ctx context.Context) error {
	var sessions []Session
	if err := r.db.WithContext(ctx).Where("status = ?", SessionStatusClosing).Find(&sessions).Error; err != nil {
		return fmt.Errorf("loading closing sessions: %w", err)
	}

	for i := range sessions {
		if err := r.closeSession(ctx, &sessions[i], nodeapi.ReasonRetry); err != nil {
			r.log.Warn().Err(err).Str("session", sessions[i].ID).Msg("close still failing")
		}
	}

	return nil
}

func (r *Runtime) syncUsage(ctx context.Context) error {
	if err := r.retryClosingSessions(ctx); err != nil {
		return err
	}

	var sessions []Session
	if err := r.db.WithContext(ctx).Where("status = ?", SessionStatusActive).Find(&sessions).Error; err != nil {
		return fmt.Errorf("loading active sessions: %w", err)
	}

	now := time.Now().Unix()
	r.refreshLeaseHealth(ctx, now)

	for i := range sessions {
		sess := &sessions[i]

		rxCounter, txCounter := sess.RxCounter, sess.TxCounter

		var rxDelta, txDelta uint64
		if stats, err := r.service.PeerStats(ctx, service.PeerID(sess.PeerID)); err == nil {
			rxDelta, txDelta = foldStats(sess, stats, now)
		} else {
			r.log.Warn().Err(err).Str("session", sess.ID).Msg("reading peer stats")
		}

		if rxDelta > 0 {
			r.metrics.sessionBytes.WithLabelValues(directionRx).Add(float64(rxDelta))
		}

		if txDelta > 0 {
			r.metrics.sessionBytes.WithLabelValues(directionTx).Add(float64(txDelta))
		}

		sess.Seconds = uint64(max(now-sess.CreatedUnix, 0))
		if rxCounter != sess.RxCounter || txCounter != sess.TxCounter ||
			session.Basis(sess.Basis) == session.BasisPerHour {
			updates := map[string]any{
				"last_traffic_unix": sess.LastTrafficUnix,
				"rx_bytes":          sess.RxBytes,
				"rx_counter":        sess.RxCounter,
				"seconds":           sess.Seconds,
				"tx_bytes":          sess.TxBytes,
				"tx_counter":        sess.TxCounter,
				"updated_unix":      now,
			}

			result := r.db.WithContext(ctx).Model(&Session{}).
				Where("id = ? AND status = ?", sess.ID, SessionStatusActive).Updates(updates)
			if result.Error != nil {
				r.log.Warn().Err(result.Error).Str("session", sess.ID).Msg("updating session usage")

				continue
			}

			if result.RowsAffected == 0 {
				continue
			}
		}

		if sess.Credential == CredentialTicket && rxDelta+txDelta > 0 {
			delta := gorm.Expr("consumed_bytes + ?", rxDelta+txDelta)
			if err := r.db.WithContext(ctx).Model(&Ticket{}).
				Where("hash = ?", sess.Subject).Update("consumed_bytes", delta).Error; err != nil {
				r.log.Warn().Err(err).Str("ticket", sess.Subject).Msg("updating ticket consumption")
			}
		}

		if reason := r.closeReason(sess, sess.Usage(), now); reason != "" {
			if err := r.closeSession(ctx, sess, reason); err != nil {
				r.log.Warn().Err(err).Str("session", sess.ID).Msg("closing exhausted session")
			}

			continue
		}

		if err := r.requestChannelVoucher(ctx, sess); err != nil {
			r.log.Warn().Err(err).Str("session", sess.ID).Msg("requesting channel voucher")
		}
	}

	r.requestLeaseVouchers(ctx, now)

	return nil
}

func (r *Runtime) updateMeter(ctx context.Context, subject string, paid uint64) {
	var sess Session
	if err := r.db.WithContext(ctx).
		Where("subject = ? AND status = ?", subject, SessionStatusActive).First(&sess).Error; err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if meter, ok := r.enforcers[sess.ID].(*session.Meter); ok {
		meter.Update(paid)
	}
}

// foldLeftoverUsage meters what active sessions used while the daemon was down. A kernel WireGuard
// peer keeps forwarding after the process dies, so its counters are read here, before
// clearLeftovers removes the old interface, and folded into each session as an ordinary sync
// would. The replacement interface starts its counters at zero, so the stored counters are reset to
// match.
func (r *Runtime) foldLeftoverUsage(ctx context.Context) {
	reader, ok := r.service.(service.LeftoverStatsReader)
	if !ok {
		return
	}

	stats, err := reader.LeftoverPeerStats(ctx)
	if err != nil {
		r.log.Debug().Err(err).Msg("no leftover peer counters to meter")

		return
	}

	var sessions []Session
	if err := r.db.WithContext(ctx).Where("status = ?", SessionStatusActive).Find(&sessions).Error; err != nil {
		r.log.Warn().Err(err).Msg("loading active sessions to meter leftover traffic")

		return
	}

	now := time.Now().Unix()

	for i := range sessions {
		sess := &sessions[i]

		peer, ok := stats[service.PeerID(sess.PeerID)]
		if !ok {
			continue
		}

		rxDelta, txDelta := foldStats(sess, peer, now)

		updates := map[string]any{
			"last_traffic_unix": sess.LastTrafficUnix,
			"rx_bytes":          sess.RxBytes,
			"rx_counter":        uint64(0),
			"tx_bytes":          sess.TxBytes,
			"tx_counter":        uint64(0),
			"updated_unix":      now,
		}
		if err := r.db.WithContext(ctx).Model(&Session{}).
			Where("id = ? AND status = ?", sess.ID, SessionStatusActive).Updates(updates).Error; err != nil {
			r.log.Warn().Err(err).Str("session", sess.ID).Msg("saving leftover traffic")

			continue
		}

		if sess.Credential == CredentialTicket && rxDelta+txDelta > 0 {
			delta := gorm.Expr("consumed_bytes + ?", rxDelta+txDelta)
			if err := r.db.WithContext(ctx).Model(&Ticket{}).
				Where("hash = ?", sess.Subject).Update("consumed_bytes", delta).Error; err != nil {
				r.log.Warn().Err(err).Str("ticket", sess.Subject).Msg("updating ticket consumption")
			}
		}

		r.log.Info().Str("session", sess.ID).Uint64("rx_bytes", rxDelta).Uint64("tx_bytes", txDelta).
			Msg("metered traffic carried while the daemon was down")
	}
}

func counterDelta(current, previous uint64) uint64 {
	if current < previous {
		return current
	}

	return current - previous
}

func foldStats(sess *Session, stats service.PeerStats, now int64) (uint64, uint64) {
	rxDelta := counterDelta(stats.RxBytes, sess.RxCounter)
	txDelta := counterDelta(stats.TxBytes, sess.TxCounter)

	if rxDelta+txDelta >= idleNoiseFloorBytes {
		sess.LastTrafficUnix = now
	}

	sess.RxBytes += rxDelta
	sess.RxCounter = stats.RxBytes
	sess.TxBytes += txDelta
	sess.TxCounter = stats.TxBytes

	return rxDelta, txDelta
}
