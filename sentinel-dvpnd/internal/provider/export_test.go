//go:build linux

package provider

import (
	"context"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/providerapi"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	ActionFund         = actionFund
	ActionOpen         = actionOpen
	ActionStartClose   = actionStartClose
	ProbeNodesInterval = probeNodesInterval
)

type Action = action

func NewAction(kind actionKind, amount uint64) Action {
	return action{amount: amount, kind: kind}
}

func AddRosterNodes(ctx context.Context, r *Runtime, plan solana.PublicKey, nodes []solana.PublicKey) error {
	return r.addRosterNodes(ctx, plan, nodes)
}

func APIHandler(r *Runtime) http.Handler { return r.apiServer.Engine() }

func CloseStreamLease(ctx context.Context, r *Runtime, lease *Lease) error {
	return r.closeStreamLease(ctx, lease)
}

func FinalizeClosing(ctx context.Context, r *Runtime) error { return r.finalizeClosing(ctx) }

func HandleLeaseViolation(ctx context.Context, r *Runtime, lease Lease, probe Probe, err error) {
	r.handleLeaseViolation(ctx, lease, probe, err)
}

func IssueTicket(ctx context.Context, r *Runtime, encoded string) (providerapi.IssueTicketResponse, error) {
	return r.issueTicket(ctx, encoded)
}

func KeepLeases(ctx context.Context, r *Runtime) error { return r.keepLeases(ctx) }

func KeepRunning(r *Runtime) bool {
	if r.keepMu.TryLock() {
		r.keepMu.Unlock()

		return false
	}

	return true
}

func LeaseActions(
	lease *Lease,
	live bool,
	deadSinceUnix int64,
	ratePerHour uint64,
	cfg *config.Provider,
	now int64,
) []Action {
	return leaseActions(lease, live, deadSinceUnix, ratePerHour, cfg, now)
}

func ProbeNodes(ctx context.Context, r *Runtime) error { return r.probeNodes(ctx) }

func ReserveTicket(
	ctx context.Context,
	r *Runtime,
	request auth.SignedTicketRequest,
	sub dvpn.Subscription,
	nonce auth.Nonce,
	now time.Time,
	expiresUnix int64,
) (auth.Ticket, error) {
	return r.reserveTicket(ctx, request, sub, nonce, now, expiresUnix)
}

func SetLogger(r *Runtime, log zerolog.Logger) { r.log = log }

func StartCloseLease(ctx context.Context, r *Runtime, lease *Lease) error {
	return r.startCloseLease(ctx, lease)
}

func StartLeaseWatcher(ctx context.Context, r *Runtime, lease Lease, probe Probe) {
	r.startLeaseWatcher(ctx, lease, probe)
}

func StopLeaseWatcher(r *Runtime, address string) { r.stopLeaseWatcher(address) }

func SyncLeases(ctx context.Context, r *Runtime) error { return r.syncLeases(ctx) }

func WatcherRunning(r *Runtime, address string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.watchers[address] != nil
}

// SetStartChainRetry shortens the start-up waits for a busy RPC; the returned func restores them.
func SetStartChainRetry(first, maxDelay time.Duration) (restore func()) {
	oldFirst, oldMax := startChainRetryFirst, startChainRetryMax
	startChainRetryFirst, startChainRetryMax = first, maxDelay

	return func() { startChainRetryFirst, startChainRetryMax = oldFirst, oldMax }
}

func SyncChain(ctx context.Context, r *Runtime) error {
	_, err := r.syncChain(ctx)

	return err
}
