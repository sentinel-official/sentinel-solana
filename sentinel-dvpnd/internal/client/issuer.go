package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
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

		now := wallNow()

		r.mu.Lock()
		if event.heartbeat || event.request != nil || event.closed {
			conn.lastContact = monoNow()
		}

		mono := monoNow()
		timedOut := conn.nodeTimedOut(mono, time.Duration(r.cfg.NodeTimeoutSeconds)*time.Second)
		contact, changed := r.markContactLocked(conn, mono)
		r.mu.Unlock()

		if changed {
			r.notify(&contact)
		}

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

// markContactLocked reports a session the node cannot be reached for as reconnecting, long before the node timeout
// redials it: the first failing poll, or no contact (heartbeat, poll, voucher) for two poll intervals. Without it
// status says active through a whole node outage while traffic is dead. The next contact makes it active again.
// Returns the session and whether its status changed (the caller notifies watchers outside r.mu).
// now is a monoNow() reading, like the contact times it is compared with.
func (r *Runtime) markContactLocked(conn *connection, now time.Duration) (Session, bool) {
	if conn.session.Status != SessionStatusActive && conn.session.Status != SessionStatusReconnecting {
		return conn.session, false
	}

	stale := max(2*time.Second, 2*r.pollInterval)
	status := SessionStatusActive

	if conn.pollFailingSince != 0 || now-conn.lastContact > stale {
		status = SessionStatusReconnecting
	}

	if conn.session.Status == status {
		return conn.session, false
	}

	conn.session.Status = status
	if r.connection == conn {
		r.publishLocked()
	}

	return conn.session, true
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
		if conn.pollFailingSince == 0 {
			conn.pollFailingSince = monoNow()
		}

		return fmt.Errorf("getting session: %w", err)
	}

	if response.JSON200 == nil {
		if response.StatusCode() >= 500 && conn.pollFailingSince == 0 {
			conn.pollFailingSince = monoNow()
		}

		return api.ParseError(response.StatusCode(), response.Body)
	}

	conn.pollFailingSince = 0

	if response.JSON200.Status != nodeapi.SessionStatusActive {
		if response.JSON200.CloseReason == "" {
			return fmt.Errorf("%w: %s session: missing reason", api.ErrInvalidResponse, response.JSON200.Status)
		}

		conn.remoteClosed = response.JSON200.Status == nodeapi.SessionStatusClosed

		return &sessionNotActiveError{reason: remoteCloseReason(conn, string(response.JSON200.CloseReason))}
	}

	r.mu.Lock()
	conn.lastContact = monoNow()
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

	redial := conn.session
	placeholder := &connection{cancel: conn.cancel, replaces: append(slices.Clone(conn.replaces), conn.session.ID), redialOf: &redial}
	r.connection = placeholder
	r.publishLocked()

	if r.firewallState.present {
		r.firewallState.kept = true
	}
	r.mu.Unlock()

	r.teardown(ctx, conn, reasonConnectionLost)

	backoff := r.reconnectBackoff
	for attempt := uint(1); attempt <= r.reconnectAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(backoff):
		}

		r.log.Info().Uint("attempt", attempt).Msg("reconnecting")

		next, err := r.connect(ctx, ctx, req, false, placeholder)
		if err == nil {
			return next, true
		}

		backoff *= 2
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

	// A kill switch of "on" (or "persist") must FAIL CLOSED when reconnecting gives up —
	// releasing it here left traffic going direct to the open internet while the tunnel was down (proven
	// live: a link cut with the kill switch on leaked real handshakes + inbound data on every
	// protocol). Kill switch off arms no block (connect.go only arms it when the switch is on or
	// persist), so there is nothing to keep here; "on"/"persist" keep blocking until the user disconnects.
	if r.cfg.Network.KillSwitch == config.KillSwitchOff {
		if _, err := r.releaseFirewall(ctx, false); err != nil {
			r.log.Error().Err(err).Msg("releasing the kill switch after exhausted reconnects; StopSession retries it")
		}
	} else {
		r.log.Warn().Str("kill_switch", r.cfg.Network.KillSwitch).Msg("reconnecting gave up: the kill switch is still blocking the internet; reconnect, or disconnect to restore normal internet")
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

	conn.lastContact = monoNow()
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
		if conn.voucherFailingSince == 0 {
			conn.voucherFailingSince = monoNow()
		}

		return fmt.Errorf("sending voucher: %w", err)
	}

	if response.JSON200 == nil {
		if response.StatusCode() >= 500 && conn.voucherFailingSince == 0 {
			conn.voucherFailingSince = monoNow()
		}

		r.metrics.vouchers.WithLabelValues(kindChannel, resultRejected).Inc()

		return api.ParseError(response.StatusCode(), response.Body)
	}

	conn.voucherFailingSince = 0
	conn.payer.RecordIssued(cumulative)

	r.mu.Lock()
	conn.lastContact = monoNow()
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

// The node timeout measures elapsed time on the monotonic clock. Wall-clock seconds jump when the
// system time is set (NTP step, manual change, resume from sleep): a jump forward by more than the
// timeout ended every live session at once as "node timed out" and started a redial.
var (
	monoBase   = time.Now()
	wallOffset atomic.Int64 // tests: a system time change, in ns
)

// monoNow is the monotonic time since start; never 0 (0 = not failing).
func monoNow() time.Duration { return time.Since(monoBase) + 1 }

// wallNow is the issue loop's wall time.
func wallNow() time.Time { return time.Now().Add(time.Duration(wallOffset.Load())) }

// nodeTimedOut reports whether the node has been silent, or polls or vouchers have kept failing, for
// longer than timeout. now is a monoNow() reading.
func (c *connection) nodeTimedOut(now, timeout time.Duration) bool {
	return now-c.lastContact > timeout ||
		(c.pollFailingSince != 0 && now-c.pollFailingSince > timeout) ||
		(c.voucherFailingSince != 0 && now-c.voucherFailingSince > timeout)
}
