//go:build linux

package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	removePeerAttempts = 5
	removePeerBackoff  = 2 * time.Second
	rollbackTimeout    = 15 * time.Second
)

var errMissingPeerResponse = errors.New("stored session has no peer response")

func (r *Runtime) closeSession(ctx context.Context, sess *Session, reason string) error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()

	if err := r.db.WithContext(ctx).First(sess, "id = ?", sess.ID).Error; err != nil {
		return fmt.Errorf("reloading session: %w", err)
	}

	if sess.Status == SessionStatusClosed {
		return nil
	}

	now := time.Now().Unix()
	if sess.Status != SessionStatusClosing {
		if err := r.settleSession(ctx, sess, reason, now); err != nil {
			return err
		}
	}

	if sess.Status == SessionStatusClosed {
		return nil
	}

	// The background retry runs every sync cycle, so it makes one attempt: retrying with sleeps here
	// held the node-wide closeMu for ~30 s per stuck session and every open and close queued.
	attempts := uint(removePeerAttempts)
	if reason == nodeapi.ReasonRetry {
		attempts = 1
	}

	if err := r.removePeer(ctx, service.PeerID(sess.PeerID), sess.ID, attempts); err != nil {
		return err
	}

	if sess.CloseReason != "" {
		reason = sess.CloseReason
	}

	return r.markClosed(ctx, sess, reason, now)
}

func (r *Runtime) countActiveSessions(ctx context.Context) (int64, error) {
	var count int64

	query := r.db.WithContext(ctx).Model(&Session{}).Where("status = ?", SessionStatusActive)
	if err := query.Count(&count).Error; err != nil {
		return 0, fmt.Errorf("counting active sessions: %w", err)
	}

	return count, nil
}

func (r *Runtime) createSession(ctx context.Context, sess *Session, enforcer session.Enforcer) error {
	if meter, ok := enforcer.(*session.Meter); ok {
		r.meterMu.Lock()
		defer r.meterMu.Unlock()

		var position Position
		if err := r.db.WithContext(ctx).First(&position, "address = ?", sess.Subject).Error; err != nil {
			return fmt.Errorf("loading accepted channel credit: %w", err)
		}

		meter.Update(position.Cumulative)
	}

	r.mu.Lock()
	r.enforcers[sess.ID] = enforcer
	r.mu.Unlock()

	if err := r.db.WithContext(ctx).Create(sess).Error; err != nil {
		r.mu.Lock()
		delete(r.enforcers, sess.ID)
		r.mu.Unlock()

		return fmt.Errorf("recording session: %w", err)
	}

	return nil
}

func (r *Runtime) markClosed(ctx context.Context, sess *Session, reason string, now int64) error {
	sess.Status = SessionStatusClosed

	sess.UpdatedUnix = now
	if err := r.db.WithContext(ctx).Save(sess).Error; err != nil {
		return fmt.Errorf("saving session: %w", err)
	}

	r.mu.Lock()
	delete(r.enforcers, sess.ID)
	delete(r.voucherRequests, sess.ID)
	r.mu.Unlock()

	r.log.Info().Str("session", sess.ID).Str("reason", reason).Msg("session closed")

	return nil
}

func (r *Runtime) openSession(
	ctx context.Context,
	admitted *admission,
	peer nodeapi.PeerRequest,
) (*Session, nodeapi.PeerResponse, error) {
	response, err := r.service.AddPeer(ctx, peer)
	if err != nil {
		return nil, nodeapi.PeerResponse{}, fmt.Errorf("adding peer: %w", err)
	}

	id := uuid.NewString()
	now := time.Now().Unix()
	rollbackPeer := func() error {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()

		return r.removePeer(rollbackCtx, response.ID, id, removePeerAttempts)
	}

	if admitted.voucher != nil && admitted.voucher.Cumulative > admitted.position.Cumulative {
		if err := r.storeVoucher(ctx, admitted.position, *admitted.voucher, now); err != nil {
			if rollbackErr := rollbackPeer(); rollbackErr != nil {
				r.log.Warn().Err(rollbackErr).Str("session", id).Msg("rolling back peer after failed voucher store")
			}

			return nil, nodeapi.PeerResponse{}, fmt.Errorf("storing voucher: %w", err)
		}
	}

	peerRequest, err := json.Marshal(peer)
	if err != nil {
		if rollbackErr := rollbackPeer(); rollbackErr != nil {
			r.log.Warn().Err(rollbackErr).Str("session", id).Msg("rolling back peer after failed session setup")
		}

		return nil, nodeapi.PeerResponse{}, fmt.Errorf("marshaling peer request: %w", err)
	}

	var peerResponse []byte
	if _, ok := r.service.(service.PeerRestorer); ok {
		peerResponse, err = json.Marshal(response)
		if err != nil {
			if rollbackErr := rollbackPeer(); rollbackErr != nil {
				r.log.Warn().Err(rollbackErr).Str("session", id).Msg("rolling back peer after failed session setup")
			}

			return nil, nodeapi.PeerResponse{}, fmt.Errorf("marshaling peer response: %w", err)
		}
	}

	sess := &Session{
		Basis:           uint8(admitted.terms.Basis),
		CreatedUnix:     now,
		Credential:      admitted.credential,
		ID:              id,
		LastTrafficUnix: now,
		PeerID:          string(response.ID),
		PeerRequest:     peerRequest,
		PeerResponse:    peerResponse,
		Price:           admitted.terms.Price,
		Signer:          admitted.signer,
		Status:          SessionStatusActive,
		Subject:         admitted.subject,
		UpdatedUnix:     now,
	}
	if err := r.createSession(ctx, sess, admitted.enforcer); err != nil {
		if rollbackErr := rollbackPeer(); rollbackErr != nil {
			r.log.Warn().Err(rollbackErr).Str("session", id).Msg("rolling back peer after failed session create")
		}

		return nil, nodeapi.PeerResponse{}, fmt.Errorf("creating session: %w", err)
	}

	if admitted.ticketHash != "" {
		if err := r.db.WithContext(ctx).Model(&Ticket{}).
			Where("hash = ?", admitted.ticketHash).Update("session", sess.ID).Error; err != nil {
			r.log.Warn().Err(err).Str("ticket", admitted.ticketHash).Msg("saving ticket session")
		}
	}

	r.metrics.sessionsActive.Inc()

	return sess, response, nil
}

func (r *Runtime) rebuildEnforcer(ctx context.Context, sess *Session) (session.Enforcer, error) {
	switch sess.Credential {
	case CredentialTicket:
		var ticket Ticket
		if err := r.db.WithContext(ctx).First(&ticket, "hash = ?", sess.Subject).Error; err != nil {
			return nil, fmt.Errorf("loading ticket: %w", err)
		}

		before := ticket.ConsumedBytes - min(ticket.ConsumedBytes, sess.RxBytes+sess.TxBytes)
		enforcer := session.NewTicketEnforcer(
			ticket.DataCapBytes-min(ticket.DataCapBytes, before), ticket.ExpiresUnix,
		)
		enforcer.SetLeaseHealthy(ticket.LeaseHealthy)

		return enforcer, nil
	default:
		var position Position
		if err := r.db.WithContext(ctx).First(&position, "address = ?", sess.Subject).Error; err != nil {
			return nil, fmt.Errorf("loading position: %w", err)
		}

		if position.Mode == dvpn.PositionModeStream {
			return session.NewStreamEnforcer(
				position.Escrow, position.StreamBase, position.RatePerHour,
				position.StreamStartedUnix,
			), nil
		}

		terms := session.Terms{
			Basis: session.Basis(sess.Basis),
			Price: sess.Price,
		}

		return session.NewMeter(terms, position.Cumulative, position.Settled), nil
	}
}

func (r *Runtime) reloadSession(ctx context.Context, sess *Session) error {
	enforcer, err := r.rebuildEnforcer(ctx, sess)
	if err != nil {
		return err
	}

	var peer nodeapi.PeerRequest
	if err := json.Unmarshal(sess.PeerRequest, &peer); err != nil {
		return fmt.Errorf("unmarshaling stored peer request: %w", err)
	}

	if restorer, ok := r.service.(service.PeerRestorer); ok {
		if len(sess.PeerResponse) == 0 {
			return errMissingPeerResponse
		}

		var response nodeapi.PeerResponse
		if err := json.Unmarshal(sess.PeerResponse, &response); err != nil {
			return fmt.Errorf("unmarshaling stored peer response: %w", err)
		}

		if err := restorer.RestorePeer(ctx, peer, response); err != nil {
			return fmt.Errorf("restoring peer: %w", err)
		}
	} else {
		response, err := r.service.AddPeer(ctx, peer)
		if err != nil {
			return fmt.Errorf("adding peer: %w", err)
		}

		if string(response.ID) != sess.PeerID {
			sess.PeerID = string(response.ID)
			if err := r.db.WithContext(ctx).Save(sess).Error; err != nil {
				return fmt.Errorf("saving session: %w", err)
			}
		}
	}

	r.mu.Lock()
	r.enforcers[sess.ID] = enforcer
	r.mu.Unlock()

	return nil
}

func (r *Runtime) reloadSessions(ctx context.Context) error {
	var sessions []Session
	if err := r.db.WithContext(ctx).Where("status = ?", SessionStatusActive).
		Order("created_unix, id").Find(&sessions).Error; err != nil {
		return fmt.Errorf("loading active sessions: %w", err)
	}

	now := time.Now().Unix()

	for i := range sessions {
		sess := &sessions[i]
		if err := r.reloadSession(ctx, sess); err != nil {
			r.log.Warn().Err(err).Str("session", sess.ID).Msg("closing session that cannot be reloaded")

			r.metrics.sessionsActive.Inc()

			if err := r.settleSession(ctx, sess, nodeapi.ReasonNoEnforcer, now); err != nil {
				r.log.Warn().Err(err).Str("session", sess.ID).Msg("settling session")

				continue
			}

			sess.Status = SessionStatusClosed

			sess.UpdatedUnix = now
			if err := r.db.WithContext(ctx).Save(sess).Error; err != nil {
				r.log.Warn().Err(err).Str("session", sess.ID).Msg("saving closed session")
			}

			continue
		}

		r.metrics.sessionsActive.Inc()
	}

	return nil
}

func (r *Runtime) removePeer(ctx context.Context, id service.PeerID, sessionID string, attempts uint) error {
	// "Not found" means the peer is already gone (the OpenVPN client left, an earlier attempt
	// removed it, or the node restarted): the goal is reached. Retrying it looped the close and the
	// client's DELETE never got an answer.
	attempt := func() error {
		if err := r.service.RemovePeer(ctx, id); err != nil && !errors.Is(err, service.ErrPeerNotFound) {
			return err
		}

		return nil
	}

	warn := func(_ uint, err error) {
		r.log.Warn().Err(err).Str("session", sessionID).Msg("removing peer; will retry")
	}
	if err := retry.Do(
		attempt,
		retry.Attempts(attempts),
		retry.Context(ctx),
		retry.Delay(removePeerBackoff),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(warn),
	); err != nil {
		return fmt.Errorf("removing peer for session %s: %w", sessionID, err)
	}

	return nil
}

func (r *Runtime) replaceSubjectSessions(ctx context.Context, subject string) error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()

	return r.replaceSubjectSessionsLocked(ctx, subject)
}

func (r *Runtime) replaceSubjectSessionsLocked(ctx context.Context, subject string) error {
	var sessions []Session
	if err := r.db.WithContext(ctx).
		Where("subject = ? AND status = ?", subject, SessionStatusActive).Find(&sessions).Error; err != nil {
		return fmt.Errorf("loading subject sessions: %w", err)
	}

	for i := range sessions {
		sess := &sessions[i]
		now := time.Now().Unix()

		if err := r.settleSession(ctx, sess, nodeapi.ReasonReplaced, now); err != nil {
			return fmt.Errorf("settling session %s: %w", sess.ID, err)
		}

		err := r.service.RemovePeer(ctx, service.PeerID(sess.PeerID))
		if err == nil {
			err = r.markClosed(ctx, sess, nodeapi.ReasonReplaced, now)
		}

		if err != nil {
			r.log.Warn().Err(err).Str("session", sess.ID).Msg("closing replaced session; will retry")
		}
	}

	return nil
}

func (r *Runtime) settleSession(ctx context.Context, sess *Session, reason string, now int64) error {
	next := *sess

	next.CloseReason = reason
	if stats, err := r.service.PeerStats(ctx, service.PeerID(sess.PeerID)); err == nil {
		foldStats(&next, stats, now)
	}

	next.Seconds = uint64(max(now-sess.CreatedUnix, 0))
	basis := session.Basis(sess.Basis)
	owed := session.Owed(basis, sess.Price, next.Usage().Amount(basis))
	won := false

	settle := func(dbtx *gorm.DB) error {
		updates := map[string]any{
			"close_reason":      reason,
			"last_traffic_unix": next.LastTrafficUnix,
			"rx_bytes":          next.RxBytes,
			"rx_counter":        next.RxCounter,
			"seconds":           next.Seconds,
			"settled_unix":      now,
			"status":            SessionStatusClosing,
			"tx_bytes":          next.TxBytes,
			"tx_counter":        next.TxCounter,
			"updated_unix":      now,
		}

		result := dbtx.Model(&Session{}).Where("id = ? AND status = ?", sess.ID, SessionStatusActive).Updates(updates)
		if result.Error != nil {
			return result.Error
		}

		won = result.RowsAffected == 1
		if !won {
			return nil
		}

		return dbtx.Model(&Position{}).
			Where("address = ? AND kind = ? AND mode = ?", sess.Subject, KindChannel, dvpn.PositionModeVoucher).
			Update("settled", gorm.Expr("MIN(settled + ?, cumulative)", owed)).Error
	}
	if err := r.db.WithContext(ctx).Transaction(settle); err != nil {
		return fmt.Errorf("settling session: %w", err)
	}

	if !won {
		if err := r.db.WithContext(ctx).First(sess, "id = ?", sess.ID).Error; err != nil {
			return fmt.Errorf("reloading session: %w", err)
		}

		return nil
	}

	*sess = next
	sess.SettledUnix = now
	sess.Status = SessionStatusClosing
	sess.UpdatedUnix = now

	r.metrics.sessionsActive.Dec()

	if err := r.publishClosed(sess.ID, reason); err != nil {
		r.log.Warn().Err(err).Str("session", sess.ID).Msg("publishing closed event")
	}

	return nil
}
