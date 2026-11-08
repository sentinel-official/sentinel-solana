package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
)

const (
	maxStatsFailures  = 5
	maxStreamBackoff  = 10 * time.Second
	minStreamBackoff  = time.Second
	missedHeartbeats  = 3
	pollInterval      = 5 * time.Second
	reconnectAttempts = 5
	reconnectBackoff  = 2 * time.Second
	sampleInterval    = time.Second
)

var errStatsUnavailable = errors.New("service stats unavailable")

type sessionNotActiveError struct {
	reason string
}

func (e *sessionNotActiveError) Error() string {
	return "remote session is not active: " + e.reason
}

func (r *Runtime) auditAndIssue(
	ctx context.Context,
	conn *connection,
	consumption nodeapi.Consumption,
	local session.Usage,
	now time.Time,
) error {
	auditErr := r.recordConsumption(conn, consumption, local, now)

	saveErr := r.saveSession(ctx, conn)
	if auditErr != nil {
		return errors.Join(auditErr, saveErr)
	}

	return errors.Join(saveErr, r.issueWhenDue(ctx, conn, local, now))
}

func (r *Runtime) handleVoucherRequest(
	ctx context.Context,
	conn *connection,
	event *nodeapi.VoucherRequestEvent,
) error {
	now := time.Now()

	local, err := r.sample(ctx, conn, now)
	if err != nil {
		return err
	}

	return r.auditAndIssue(ctx, conn, event.Consumption, local, now)
}

func (r *Runtime) issueLoop(ctx context.Context, conn *connection) (string, bool) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := make(chan nodeEvent)
	go r.readEvents(streamCtx, conn, events)

	sampleTicker := time.NewTicker(r.sampleInterval)
	defer sampleTicker.Stop()

	pollTicker := time.NewTicker(r.pollInterval)
	defer pollTicker.Stop()

	for {
		var (
			event nodeEvent
			poll  bool
		)

		select {
		case <-ctx.Done():
			return "", false
		case event = <-events:
		case <-sampleTicker.C:
		case <-pollTicker.C:
			poll = true
		}

		if errors.Is(event.err, api.ErrUnauthorized) {
			return "", true
		}

		if reason := issueErrorReason(event.err); reason != "" {
			return reason, false
		}

		conn.issueMu.Lock()

		now := time.Now()

		r.mu.Lock()
		if event.heartbeat || event.request != nil || event.closed {
			conn.lastContactUnix = now.Unix()
		}

		timeout := int64(r.cfg.NodeTimeoutSeconds)
		timedOut := now.Unix()-conn.lastContactUnix > timeout ||
			(conn.pollFailingSinceUnix != 0 && now.Unix()-conn.pollFailingSinceUnix > timeout) ||
			(conn.voucherFailingSinceUnix != 0 && now.Unix()-conn.voucherFailingSinceUnix > timeout)
		r.mu.Unlock()

		if timedOut {
			conn.issueMu.Unlock()

			return "", true
		}

		if event.closed {
			reason := remoteCloseReason(conn, event.reason)
			conn.issueMu.Unlock()

			return reason, false
		}

		var err error

		switch {
		case event.request != nil:
			err = r.handleVoucherRequest(ctx, conn, event.request)
		case poll:
			err = r.poll(ctx, conn)
		case !event.heartbeat:
			err = r.sampleAndIssue(ctx, conn, now)
		}
		conn.issueMu.Unlock()

		if reason := issueErrorReason(err); reason != "" {
			return reason, false
		}

		if err != nil {
			r.log.Warn().Err(err).Msg("issuing voucher failed")
		}
	}
}

func (r *Runtime) issueWhenDue(ctx context.Context, conn *connection, local session.Usage, now time.Time) error {
	if conn.payer == nil {
		return nil
	}

	cumulative, issue := conn.payer.Next(local, conn.nodeUsage)
	if !issue {
		return nil
	}

	return r.sendVoucher(ctx, conn, cumulative, now)
}

func (r *Runtime) poll(ctx context.Context, conn *connection) error {
	pctx, cancel := r.withTimeout(ctx)
	defer cancel()

	response, err := conn.nodeClient.GetSessionWithResponse(pctx, conn.session.ID)
	now := time.Now()

	if err != nil {
		if conn.pollFailingSinceUnix == 0 {
			conn.pollFailingSinceUnix = now.Unix()
		}

		return fmt.Errorf("getting session: %w", err)
	}

	if response.JSON200 == nil {
		if response.StatusCode() >= 500 && conn.pollFailingSinceUnix == 0 {
			conn.pollFailingSinceUnix = now.Unix()
		}

		return api.ParseError(response.StatusCode(), response.Body)
	}

	conn.pollFailingSinceUnix = 0

	if response.JSON200.Status != nodeapi.SessionStatusActive {
		if response.JSON200.CloseReason == "" {
			return fmt.Errorf("%w: %s session: missing reason", api.ErrInvalidResponse, response.JSON200.Status)
		}

		conn.remoteClosed = response.JSON200.Status == nodeapi.SessionStatusClosed

		return &sessionNotActiveError{reason: remoteCloseReason(conn, string(response.JSON200.CloseReason))}
	}

	r.mu.Lock()
	conn.lastContactUnix = now.Unix()
	r.mu.Unlock()

	local, err := r.sample(ctx, conn, now)
	if err != nil {
		return err
	}

	return r.auditAndIssue(ctx, conn, response.JSON200.Consumption, local, now)
}

func (r *Runtime) reconnect(
	ctx context.Context,
	conn *connection,
	req *daemonv1.StartSessionRequest,
) (*connection, bool) {
	r.mu.Lock()
	if r.connection != conn {
		r.mu.Unlock()

		return nil, false
	}

	placeholder := &connection{cancel: conn.cancel}
	r.connection = placeholder
	r.publishLocked()

	if r.firewallState.present {
		r.firewallState.kept = true
	}
	r.mu.Unlock()

	r.teardown(ctx, conn, reasonConnectionLost)

	var next *connection

	redialed := newRedialPolicy(r.reconnectAttempts, r.reconnectBackoff).run(ctx, r.networkUp,
		func(attempt uint, waitingForNetwork bool) error {
			if waitingForNetwork {
				r.log.Info().Uint("attempt", attempt).Msg("reconnecting (the network was gone)")
			} else {
				r.log.Info().Uint("attempt", attempt).Msg("reconnecting")
			}

			c, err := r.connect(ctx, ctx, req, false, placeholder)
			if err != nil {
				if networkDown(err) {
					r.log.Warn().Err(err).Msg("no network; waiting for it before using the next attempt")
				}

				return err
			}

			next = c

			return nil
		})
	if redialed {
		return next, true
	}

	if ctx.Err() != nil {
		return nil, false
	}

	r.mu.Lock()

	exhausted := r.connection == placeholder
	if exhausted {
		r.clearConnectionLocked()
	}

	r.mu.Unlock()

	if !exhausted {
		return nil, false
	}

	if r.cfg.Network.KillSwitch != config.KillSwitchPersist {
		if _, err := r.releaseFirewall(ctx, false); err != nil {
			r.log.Error().Err(err).Msg("releasing the kill switch after exhausted reconnects; StopSession retries it")
		}
	}

	r.log.Error().Msg("reconnect attempts exhausted")

	return nil, false
}

func (r *Runtime) recordConsumption(
	conn *connection,
	consumption nodeapi.Consumption,
	local session.Usage,
	now time.Time,
) error {
	if conn.payer != nil {
		if err := conn.payer.Audit(
			local, consumption.Usage, consumption.SettledBaseUnits, consumption.OwedBaseUnits,
		); err != nil {
			return err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	conn.nodeUsage = consumption.Usage
	conn.nodeOwed = consumption.OwedBaseUnits
	conn.nodeSettled = consumption.SettledBaseUnits

	conn.lastContactUnix = now.Unix()
	if r.connection == conn {
		conn.session.SpentBaseUnits = max(
			conn.session.SpentBaseUnits,
			consumption.OwedBaseUnits-min(consumption.OwedBaseUnits, consumption.SettledBaseUnits),
		)
		conn.session.UpdatedUnix = now.Unix()

		r.publishLocked()
	}

	return nil
}

func (r *Runtime) sample(ctx context.Context, conn *connection, now time.Time) (session.Usage, error) {
	sctx, cancel := r.withTimeout(ctx)
	defer cancel()

	stats, err := conn.service.Stats(sctx)
	if err != nil {
		conn.statsFailures++
		if conn.statsFailures >= maxStatsFailures {
			return session.Usage{}, fmt.Errorf("%w: %w", errStatsUnavailable, err)
		}

		return session.Usage{}, fmt.Errorf("reading peer stats: %w", err)
	}

	conn.statsFailures = 0

	r.mu.Lock()
	defer r.mu.Unlock()

	rx := stats.RxBytes - min(stats.RxBytes, conn.session.RxBytes)
	tx := stats.TxBytes - min(stats.TxBytes, conn.session.TxBytes)

	r.metrics.sessionBytes.WithLabelValues(directionRx).Add(float64(rx))
	r.metrics.sessionBytes.WithLabelValues(directionTx).Add(float64(tx))

	conn.session.RxBytes = max(conn.session.RxBytes, stats.RxBytes)
	conn.session.TxBytes = max(conn.session.TxBytes, stats.TxBytes)
	conn.session.Seconds = uint64(max(0, now.Unix()-conn.session.StartedUnix))
	conn.session.UpdatedUnix = now.Unix()

	if conn.payer != nil {
		owed := session.Owed(conn.terms.Basis, conn.terms.Price, conn.session.Usage().Amount(conn.terms.Basis))
		conn.session.SpentBaseUnits = max(conn.session.SpentBaseUnits, owed)
	}

	if r.connection == conn {
		r.publishLocked()
	}

	return conn.session.Usage(), nil
}

func (r *Runtime) sampleAndIssue(ctx context.Context, conn *connection, now time.Time) error {
	local, err := r.sample(ctx, conn, now)
	if err != nil {
		return err
	}

	saveErr := r.saveSession(ctx, conn)

	return errors.Join(saveErr, r.issueWhenDue(ctx, conn, local, now))
}

func (r *Runtime) saveSession(ctx context.Context, conn *connection) error {
	r.mu.Lock()
	if r.connection != conn {
		r.mu.Unlock()

		return nil
	}

	if conn.payer != nil {
		conn.session.PaidBaseUnits = conn.payer.Paid()
		conn.session.SettledBaseUnits = conn.nodeSettled

		r.publishLocked()
	}

	snapshot := conn.session
	r.mu.Unlock()

	if err := r.save(ctx, &snapshot); err != nil {
		return err
	}

	r.notify(&snapshot)

	return nil
}

func (r *Runtime) saveVoucher(
	ctx context.Context,
	conn *connection,
	voucher nodeapi.Voucher,
	issued, accepted time.Time,
) error {
	conn.channel.Cumulative = conn.payer.Paid()
	conn.channel.UpdatedUnix = accepted.Unix()

	r.mu.Lock()
	conn.session.PaidBaseUnits = conn.payer.Paid()
	conn.session.SettledBaseUnits = conn.nodeSettled

	snapshot := conn.session
	if r.connection == conn {
		r.publishLocked()
	}
	r.mu.Unlock()

	row := Voucher{
		AcceptedUnix:   accepted.Unix(),
		ChannelAddress: conn.channel.Address,
		Cumulative:     voucher.Cumulative,
		IssuedUnix:     issued.Unix(),
		SessionID:      snapshot.ID,
		Signature:      voucher.Signature,
	}

	pctx, cancel := r.withDetachedTimeout(ctx)
	defer cancel()

	if err := r.db.WithContext(pctx).Transaction(func(db *gorm.DB) error {
		if err := db.Create(&row).Error; err != nil {
			return fmt.Errorf("recording accepted voucher: %w", err)
		}

		if err := db.Save(conn.channel).Error; err != nil {
			return fmt.Errorf("recording channel paid exposure: %w", err)
		}

		return db.Save(&snapshot).Error
	}); err != nil {
		return fmt.Errorf("saving accepted voucher: %w", err)
	}

	r.notify(&snapshot)

	return nil
}

func (r *Runtime) sendVoucher(
	ctx context.Context,
	conn *connection,
	cumulative uint64,
	now time.Time,
) error {
	address, _ := solana.PublicKeyFromBase58(conn.channel.Address)
	voucher := r.signVoucher(conn.channel, cumulative)
	body := nodeapi.SendVoucherRequest{Voucher: voucher}

	vctx, cancel := r.withTimeout(ctx)
	defer cancel()

	response, err := conn.nodeClient.SendChannelVoucherWithResponse(vctx, address, body)
	accepted := time.Now()

	if err != nil {
		if conn.voucherFailingSinceUnix == 0 {
			conn.voucherFailingSinceUnix = accepted.Unix()
		}

		return fmt.Errorf("sending voucher: %w", err)
	}

	if response.JSON200 == nil {
		if response.StatusCode() >= 500 && conn.voucherFailingSinceUnix == 0 {
			conn.voucherFailingSinceUnix = accepted.Unix()
		}

		r.metrics.vouchers.WithLabelValues(kindChannel, resultRejected).Inc()

		return api.ParseError(response.StatusCode(), response.Body)
	}

	conn.voucherFailingSinceUnix = 0
	conn.payer.RecordIssued(cumulative)

	r.mu.Lock()
	conn.lastContactUnix = accepted.Unix()
	r.mu.Unlock()

	r.metrics.vouchers.WithLabelValues(kindChannel, resultAccepted).Inc()
	saveErr := r.saveVoucher(ctx, conn, voucher, now, accepted)

	local, err := r.sample(ctx, conn, accepted)
	if err != nil {
		return errors.Join(saveErr, err)
	}

	auditErr := r.recordConsumption(conn, response.JSON200.Consumption, local, accepted)

	return errors.Join(saveErr, auditErr, r.saveSession(ctx, conn))
}

func (r *Runtime) supervise(ctx context.Context, conn *connection, req *daemonv1.StartSessionRequest) {
	for {
		reason, reconnect := r.issueLoop(ctx, conn)

		switch {
		case reconnect:
			next, ok := r.reconnect(ctx, conn, req)
			if !ok {
				return
			}

			conn = next
		case reason != "":
			r.stopIfCurrent(ctx, conn, reason)

			return
		default:
			return
		}
	}
}

func isInvalidResponse(err error) bool {
	return errors.Is(err, api.ErrInvalidResponse) || errors.Is(err, api.ErrResponseTooLarge) ||
		errors.Is(err, api.ErrUnexpectedRedirect)
}

func issueErrorReason(err error) string {
	if notActive, ok := errors.AsType[*sessionNotActiveError](err); ok {
		return notActive.reason
	}

	switch {
	case errors.Is(err, session.ErrUsageMismatch), errors.Is(err, session.ErrSettledChanged),
		errors.Is(err, session.ErrOwedMismatch):
		return reasonNodeOvercharging
	case isInvalidResponse(err):
		return reasonInvalidResponse
	case errors.Is(err, errStatsUnavailable):
		return reasonStatsUnavailable
	case errors.Is(err, api.ErrChannelNotOpen), errors.Is(err, api.ErrSessionNotFound):
		return err.Error()
	default:
		return ""
	}
}

func remoteCloseReason(conn *connection, reason string) string {
	if reason == nodeapi.ReasonCreditExhausted && conn.payer != nil && conn.terms.Price > 0 &&
		conn.payer.Paid() == conn.channel.Escrow {
		return reasonEscrowExhausted
	}

	return reason
}
