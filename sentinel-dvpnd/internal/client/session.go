package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
)

const (
	reasonConnectionLost   = "connection lost"
	reasonDaemonRestarted  = "daemon restarted"
	reasonDaemonStopping   = "daemon stopping"
	reasonEscrowExhausted  = "escrow exhausted"
	reasonInvalidResponse  = "invalid node response"
	reasonNodeOvercharging = "node overcharging"
	reasonStatsUnavailable = "service stats unavailable"
	reasonStoppedByUser    = "stopped by user"
)

var (
	ErrSessionActive        = errors.New("a session is already active")
	ErrStoppedDuringConnect = errors.New("stop requested while connecting")
)

var (
	errDaemonStopping = errors.New("the daemon is stopping")
	errMissingSession = errors.New("no active session")
)

type connection struct {
	block            uint64
	cancel           context.CancelFunc
	channel          *Channel
	heartbeat        time.Duration
	iface            string
	issueMu          sync.Mutex
	lastContact      time.Duration // monoNow() of the last node answer
	minBlock         uint64
	nodeClient       *nodeapi.ClientWithResponses
	nodeOwed         uint64
	nodeSettled      uint64
	nodeUsage        session.Usage
	payer            *session.Payer
	pollFailingSince time.Duration // monoNow() of the first failed poll; 0 = not failing
	// redialOf is set on the placeholder of an automatic reconnect: the session being redialled. Status shows it
	// as reconnecting, a stop by its ID cancels the redial, and a new start replaces the redial.
	redialOf            *Session
	remoteClosed        bool
	replaces            []string
	service             service.Client
	session             Session
	statsFailures       uint
	streamClient        *nodeapi.ClientWithResponses
	terms               session.Terms
	voucherFailingSince time.Duration // monoNow() of the first failed voucher; 0 = not failing
}

type startResult struct {
	err     error
	session *Session
}

type stopResult struct {
	dnsRevertFailed    bool
	killSwitchReleased bool
	session            *Session
}

type watcher struct {
	latest    *Session
	mu        sync.Mutex
	sessionID string
	wake      chan struct{}
}

func newWatcher(sessionID string) *watcher {
	return &watcher{
		sessionID: sessionID,
		wake:      make(chan struct{}, 1),
	}
}

func (w *watcher) offer(s *Session) {
	if w.sessionID != "" && s.ID != w.sessionID {
		return
	}

	w.mu.Lock()

	if w.latest != nil && w.latest.Status == SessionStatusEnded {
		w.mu.Unlock()

		return
	}

	copied := *s
	w.latest = &copied
	w.mu.Unlock()

	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *watcher) takeLatest() *Session {
	w.mu.Lock()
	defer w.mu.Unlock()

	s := w.latest
	w.latest = nil

	return s
}

func (r *Runtime) claim() (*connection, context.Context, turn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopping {
		return nil, nil, turn{}, errDaemonStopping
	}

	if r.connection != nil {
		if r.connection.redialOf == nil {
			return nil, nil, turn{}, ErrSessionActive
		}

		// A start while the daemon redials a lost node replaces the redial: cancelling its lifecycle ends the
		// reconnect loop, and a redial that completes anyway no longer owns the connection and is torn down.
		r.clearConnectionLocked()
	}

	lifecycle, cancel := context.WithCancel(context.Background())
	placeholder := &connection{cancel: cancel}
	r.connection = placeholder
	r.publishLocked()

	return placeholder, lifecycle, r.turns.enqueue(), nil
}

func (r *Runtime) clearConnectionLocked() {
	if r.connection != nil && r.connection.cancel != nil {
		r.connection.cancel()
	}

	r.connection = nil
	r.snapshot.Store(nil)
}

func (r *Runtime) closeRemoteSession(ctx context.Context, client *nodeapi.ClientWithResponses, sessionID string) bool {
	ctx, cancel := r.withDetachedTimeout(ctx)
	defer cancel()

	return r.closeRemoteSessionWithin(ctx, client, sessionID)
}

func (r *Runtime) closeRemoteSessionWithin(
	ctx context.Context,
	client *nodeapi.ClientWithResponses,
	sessionID string,
) bool {
	response, err := client.CloseSessionWithResponse(ctx, sessionID)
	if err != nil {
		r.log.Warn().Err(err).Str("session", sessionID).Msg("closing remote session failed")

		return false
	}

	if response.StatusCode() != http.StatusNoContent {
		r.log.Warn().Int("status", response.StatusCode()).Str("session", sessionID).Msg("remote close refused")

		return false
	}

	return true
}

func (r *Runtime) current() (*Session, bool) {
	snapshot := r.snapshot.Load()
	if snapshot == nil {
		return nil, false
	}

	copied := *snapshot

	return &copied, true
}

func (r *Runtime) notify(s *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for w := range r.watchers {
		w.offer(s)
	}
}

func (r *Runtime) owns(placeholder *connection) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.connection == placeholder
}

func (r *Runtime) publishLocked() {
	if r.connection != nil && r.connection.session.ID == "" && r.connection.redialOf != nil {
		redial := *r.connection.redialOf
		redial.Status = SessionStatusReconnecting
		r.snapshot.Store(&redial)

		return
	}

	if r.connection == nil || r.connection.session.ID == "" {
		r.snapshot.Store(nil)

		return
	}

	copied := r.connection.session
	r.snapshot.Store(&copied)
}

func (r *Runtime) runSession(
	ctx,
	lifecycle context.Context,
	req *daemonv1.StartSessionRequest,
	placeholder *connection,
	results chan<- startResult,
) {
	connectCtx, cancelConnect := context.WithCancel(ctx)
	unlink := context.AfterFunc(lifecycle, cancelConnect)

	conn, err := r.connect(connectCtx, lifecycle, req, false, placeholder)

	unlink()
	cancelConnect()

	if err != nil {
		r.mu.Lock()

		stopped := r.connection != placeholder
		if !stopped {
			r.clearConnectionLocked()
		}

		r.mu.Unlock()

		if stopped && !errors.Is(err, ErrStoppedDuringConnect) {
			err = fmt.Errorf("%w: %w", ErrStoppedDuringConnect, err)
		}

		results <- startResult{err: err}

		return
	}

	started := conn.session
	results <- startResult{session: &started}

	r.supervise(lifecycle, conn, req)
}

func (r *Runtime) startService(ctx context.Context, conn *connection, host string, peer service.PeerResponse) error {
	if err := conn.service.Start(ctx, host, peer); err != nil {
		return fmt.Errorf("starting service: %w", err)
	}

	return nil
}

func (r *Runtime) startSession(ctx context.Context, req *daemonv1.StartSessionRequest) (*Session, error) {
	placeholder, lifecycle, turn, err := r.claim()
	if err != nil {
		return nil, err
	}

	results := make(chan startResult, 1)

	runSession := func() { r.runSession(ctx, lifecycle, req, placeholder, results) }
	go turn.run(runSession)

	result := <-results

	return result.session, result.err
}

func (r *Runtime) stopIfCurrent(ctx context.Context, conn *connection, reason string) {
	r.mu.Lock()
	if r.connection != conn {
		r.mu.Unlock()

		return
	}

	r.clearConnectionLocked()
	r.mu.Unlock()

	r.teardown(ctx, conn, reason)
}

func (r *Runtime) stopService(ctx context.Context, svc service.Client) {
	ctx, cancel := r.withDetachedTimeout(ctx)
	defer cancel()

	_ = svc.Stop(ctx)
}

func (r *Runtime) stopSession(ctx context.Context, reason string) (stopResult, error) {
	return r.stopSessionByID(ctx, reason, "")
}

func (r *Runtime) stopSessionByID(ctx context.Context, reason, id string) (stopResult, error) {
	conn, turn, takeErr := r.takeConnection(reason, id)
	if takeErr != nil {
		return stopResult{}, takeErr
	}

	var (
		result stopResult
		err    error
	)

	finish := func() {
		if conn != nil {
			result = r.teardown(ctx, conn, reason)

			return
		}

		r.mu.Lock()
		present, dnsPending := r.firewallState.present, r.dnsIface != ""
		r.mu.Unlock()

		if dnsPending {
			result.dnsRevertFailed = r.revertDNS(ctx) != nil
		}

		if !present || reason == reasonDaemonStopping {
			if !dnsPending {
				err = errMissingSession
			}

			return
		}

		_, err = r.releaseFirewall(ctx, false)
		result.killSwitchReleased = err == nil
	}
	turn.run(finish)

	if conn == nil && id != "" && err == nil {
		err = errMissingSession
	}

	return result, err
}

func (r *Runtime) takeConnection(reason, id string) (*connection, turn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if reason == reasonDaemonStopping {
		r.stopping = true
	}

	conn := r.connection
	// A redial placeholder carries the redialled id in replaces, so a stop by it also cancels the redial.
	if id != "" && conn != nil && conn.session.ID != id && !slices.Contains(conn.replaces, id) {
		return nil, turn{}, errMissingSession
	}

	if conn == nil {
		return nil, r.turns.enqueue(), nil
	}

	r.clearConnectionLocked()

	return conn, r.turns.enqueue(), nil
}

func (r *Runtime) teardown(ctx context.Context, conn *connection, reason string) stopResult {
	conn.issueMu.Lock()
	defer conn.issueMu.Unlock()

	sessionID := conn.session.ID

	closed := conn.remoteClosed
	if conn.nodeClient != nil {
		closed = r.closeRemoteSession(ctx, conn.nodeClient, sessionID) || closed
	}

	finalSample := false

	if conn.service != nil {
		sampleCtx, cancel := r.withDetachedTimeout(ctx)
		_, err := r.sample(sampleCtx, conn, time.Now())

		cancel()

		finalSample = err == nil
		if err != nil {
			r.log.Warn().Err(err).Msg("sampling final session usage failed")
		}

		r.stopService(ctx, conn.service)
		r.dropPinnedConns("tunnel down")
	}

	var result stopResult

	result.dnsRevertFailed = r.revertDNS(ctx) != nil

	if !keepsFirewall(reason, r.cfg.Network.KillSwitch) {
		released, err := r.releaseFirewall(ctx, false)
		if err != nil {
			r.log.Error().Err(err).Msg("releasing the kill switch; StopSession retries it")
		}

		result.killSwitchReleased = released
	} else if r.cfg.Network.KillSwitch != config.KillSwitchOff && reason != reasonStoppedByUser {
		r.log.Warn().Str("kill_switch", r.cfg.Network.KillSwitch).Str("reason", reason).Msg("the session ended but the kill switch is still blocking the internet; disconnect to restore normal internet")
	}

	if sessionID == "" {
		return result
	}

	r.metrics.sessionsActive.Dec()
	r.mu.Lock()
	conn.session.Clean = finalSample && closed && reason != reasonConnectionLost
	conn.session.EndedUnix = time.Now().Unix()
	conn.session.Error = reason
	conn.session.Status = SessionStatusEnded
	conn.session.UpdatedUnix = conn.session.EndedUnix

	if conn.payer != nil {
		conn.session.OwedBaseUnits = session.Owed(
			conn.terms.Basis,
			conn.terms.Price,
			conn.session.Usage().Amount(conn.terms.Basis),
		)
		conn.session.PaidBaseUnits = conn.payer.Paid()
		conn.session.SettledBaseUnits = conn.nodeSettled
	}

	final := conn.session
	r.mu.Unlock()

	r.saveEndedSession(ctx, &final)
	r.notify(&final)

	result.session = &final

	return result
}

func (r *Runtime) unwatch(w *watcher) {
	r.mu.Lock()
	delete(r.watchers, w)
	r.mu.Unlock()
}

func (r *Runtime) watch(sessionID string) (*watcher, *Session) {
	w := newWatcher(sessionID)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.watchers[w] = struct{}{}

	s := r.snapshot.Load()
	if s == nil || (sessionID != "" && s.ID != sessionID) {
		return w, nil
	}

	copied := *s

	return w, &copied
}

// keepsFirewall decides whether a session end KEEPS the kill-switch block in place. Ported from the
// product decision: with "on", a session that ends for a reason the user did not ask for keeps the
// exhausted, node overcharging, the node closing or forgetting the session, a daemon restart) — no
// lift, no leak. Only the user (Disconnect / Restore network), a daemon stop (persist keeps even
// that) and kill switch off lift it. "persist" keeps the block for every end the user did not ask for.
func keepsFirewall(reason, killSwitch string) bool {
	if killSwitch == config.KillSwitchPersist && reason != reasonStoppedByUser {
		return true
	}

	switch reason {
	case reasonConnectionLost, reasonStatsUnavailable:
		return true
	case reasonDaemonStopping:
		return killSwitch == config.KillSwitchPersist
	case reasonStoppedByUser:
		return false
	default:
		return killSwitch == config.KillSwitchOn
	}
}

// liveSession returns the current session when id is its own, or the id of a session it replaced in an
// automatic reconnect (connection.replaces, oldest first): the caller keeps the id it started, so that id reads
// the live session (its own id) instead of the ended row, and StopSession accepts it too (takeConnection).
func (r *Runtime) liveSession(id string) (*Session, bool) {
	s, ok := r.current()
	if !ok {
		return nil, false
	}

	if s.ID == id {
		return s, true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	conn := r.connection

	return s, conn != nil && conn.session.ID == s.ID && slices.Contains(conn.replaces, id)
}

// dropPinnedConns closes every connection dialled through the pin (chain RPC and node API) when the
// tunnel comes up or goes down. Their sockets keep the source address and route they were opened on:
// one opened before the tunnel keeps the physical address and, once the tunnel carries the route, its
// packets go into the tunnel with a source the peer does not accept, so the request hangs until its
// timeout. The HTTP clients redial on their next request, over the current route.
func (r *Runtime) dropPinnedConns(why string) {
	if r.pin == nil {
		return
	}

	if n := r.pin.CloseConns(); n > 0 {
		r.log.Debug().Int("closed", n).Str("why", why).Msg("dropped pooled connections")
	}
}
