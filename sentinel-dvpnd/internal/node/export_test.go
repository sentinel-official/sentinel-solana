//go:build linux

package node

import (
	"context"

	"github.com/solana-foundation/solana-go/v2"

	"github.com/sentinel-official/sentinel-dvpnd/internal/leftover"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

type Admission = admission

func (a *admission) Credential() string { return a.credential }

func (a *admission) Enforcer() session.Enforcer { return a.enforcer }

func (a *admission) Signer() string { return a.signer }

func (a *admission) Subject() string { return a.subject }

func NewEventHub() *eventHub { return newEventHub() }

func (h *eventHub) Publish(topic string, frame []byte) { h.publish(topic, frame) }

func (h *eventHub) Subscribe(topic string) (<-chan []byte, func()) { return h.subscribe(topic) }

func AcceptChannelVoucher(
	ctx context.Context,
	r *Runtime,
	channel solana.PublicKey,
	signer solana.PublicKey,
	submitted nodeapi.Voucher,
) (*Position, error) {
	return r.acceptVoucher(ctx, KindChannel, channel, signer, submitted)
}

func Admit(
	ctx context.Context,
	r *Runtime,
	signer solana.PublicKey,
	req nodeapi.OpenSessionRequest,
) (*Admission, error) {
	return r.admit(ctx, signer, req)
}

func AuthorizeParty(ctx context.Context, r *Runtime, position *Position, signer solana.PublicKey) error {
	return r.authorizeParty(ctx, position, signer)
}

func BuildMetadata(ctx context.Context, r *Runtime) (nodeapi.Metadata, error) {
	return r.buildMetadata(ctx)
}

func ChainState(r *Runtime) (dvpn.Config, dvpn.Node) { return r.chainState() }

func ClearLeftovers(ctx context.Context, r *Runtime) error { return r.clearLeftovers(ctx) }

func CloseSession(ctx context.Context, r *Runtime, sess *Session, reason string) error {
	return r.closeSession(ctx, sess, reason)
}

func CurrentMetadata(r *Runtime) auth.SignedMessage { return r.metadata.Current() }

func HasEnforcer(r *Runtime, id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.enforcers[id]

	return ok
}

func LeaseHealthy(r *Runtime, position *Position, node dvpn.Node, now int64) bool {
	return r.leaseHealthy(position, node, now)
}

func MeterPaid(r *Runtime, sessionID string) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	enforcer, ok := r.enforcers[sessionID]
	if !ok {
		return 0, false
	}

	meter, ok := enforcer.(*session.Meter)
	if !ok {
		return 0, false
	}

	return meter.Paid(), true
}

func NewHandler(r *Runtime) nodeapi.StrictServerInterface { return newHandler(r) }

func OpenSession(
	ctx context.Context,
	r *Runtime,
	admitted *Admission,
	peer nodeapi.PeerRequest,
) (*Session, nodeapi.PeerResponse, error) {
	return r.openSession(ctx, admitted, peer)
}

func Publish(r *Runtime, topic string, frame []byte) { r.events.publish(topic, frame) }

func Redeem(ctx context.Context, r *Runtime) error { return r.redeem(ctx) }

func RefreshLeaseHealth(ctx context.Context, r *Runtime, now int64) {
	r.refreshLeaseHealth(ctx, now)
}

func RefreshMetadata(ctx context.Context, r *Runtime) error { return r.refreshMetadata(ctx) }

func ReloadSessions(ctx context.Context, r *Runtime) error { return r.reloadSessions(ctx) }

func RequestLeaseVouchers(ctx context.Context, r *Runtime, now int64) {
	r.requestLeaseVouchers(ctx, now)
}

func SendHeartbeat(ctx context.Context, r *Runtime) error { return r.sendHeartbeat(ctx) }

const SendHeartbeatInterval = sendHeartbeatInterval

func SetChainNode(r *Runtime, node dvpn.Node) {
	r.mu.Lock()
	r.chainNode = node
	r.mu.Unlock()
}

func SetEnforcer(r *Runtime, id string, enforcer session.Enforcer) {
	r.mu.Lock()
	r.enforcers[id] = enforcer
	r.mu.Unlock()
}

func SetInterfaceRemover(r *Runtime, remover leftover.InterfaceRemover) { r.remover = remover }

func SetServerCleanup(r *Runtime, cleanup serverCleanupFunc) { r.serverCleanup = cleanup }

func SetService(r *Runtime, srv service.Server) { r.service = srv }

func SetVoucherRequestSentUnix(r *Runtime, topic string, unix int64) {
	r.mu.Lock()
	state := r.voucherRequests[topic]
	state.sentUnix = unix
	r.voucherRequests[topic] = state
	r.mu.Unlock()
}

func Subscribe(r *Runtime, topic string) (<-chan []byte, func()) {
	return r.events.subscribe(topic)
}

func SubscriberCount(r *Runtime, topic string) int {
	r.events.mu.Lock()
	defer r.events.mu.Unlock()

	return len(r.events.subscribers[topic])
}

func SyncChain(ctx context.Context, r *Runtime) error { return r.syncChain(ctx) }

func SyncConfig(ctx context.Context, r *Runtime) error { return r.syncConfig(ctx) }

func SyncPositions(ctx context.Context, r *Runtime) error { return r.syncPositions(ctx) }

func SyncUsage(ctx context.Context, r *Runtime) error { return r.syncUsage(ctx) }
