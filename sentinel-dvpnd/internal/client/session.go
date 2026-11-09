package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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
	block                   uint64
	cancel                  context.CancelFunc
	channel                 *Channel
	heartbeat               time.Duration
	iface                   string
	issueMu                 sync.Mutex
	lastContactUnix         int64
	minBlock                uint64
	nodeClient              *nodeapi.ClientWithResponses
	nodeOwed                uint64
	nodeSettled             uint64
	nodeUsage               session.Usage
	payer                   *session.Payer
	pollFailingSinceUnix    int64
	remoteClosed            bool
	service                 service.Client
	session                 Session
	statsFailures           uint
	streamClient            *nodeapi.ClientWithResponses
	terms                   session.Terms
	voucherFailingSinceUnix int64
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
		return nil, nil, turn{}, ErrSessionActive
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
	if id != "" && conn != nil && conn.session.ID != id {
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

	// A session tearing down reads "closing" (the app shows "Disconnecting"), not "active", for the
	// whole teardown -- OpenVPN's slow close keeps stopService busy ~15 s. Persist + notify before the final sample,
	// closeRemoteSession/stopService so ListSessions (a DB poll) and watchers (the stream) both see it.
	if sessionID != "" {
		r.mu.Lock()
		conn.session.Status = SessionStatusClosing
		conn.session.UpdatedUnix = time.Now().Unix()
		snapshot := conn.session
		r.mu.Unlock()
		if err := r.save(ctx, &snapshot); err != nil {
			r.log.Warn().Err(err).Msg("marking the session closing during teardown")
		}
		r.notify(&snapshot)
	}

	// The final sample comes first: closing the session on the node ends the node's side of the tunnel (an
	// OpenVPN server drops the peer and the client's tun device goes with it), and sampling is local only.
	finalSample := false

	if conn.service != nil {
		sampleCtx, cancel := r.withDetachedTimeout(ctx)
		_, err := r.sample(sampleCtx, conn, time.Now())

		cancel()

		finalSample = err == nil
		if err != nil {
			r.log.Warn().Err(err).Msg("sampling final session usage failed")
		}
	}

	closed := conn.remoteClosed
	if conn.nodeClient != nil {
		closed = r.closeRemoteSession(ctx, conn.nodeClient, sessionID) || closed
	}

	if conn.service != nil {
		r.stopService(ctx, conn.service)
	}

	var result stopResult

	result.dnsRevertFailed = r.revertDNS(ctx) != nil

	if reason != reasonConnectionLost &&
		(reason != reasonDaemonStopping || r.cfg.Network.KillSwitch != config.KillSwitchPersist) {
		released, err := r.releaseFirewall(ctx, false)
		if err != nil {
			r.log.Error().Err(err).Msg("releasing the kill switch; StopSession retries it")
		}

		result.killSwitchReleased = released
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
