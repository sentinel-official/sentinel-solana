package client

import (
	"context"
	"net/netip"
	"time"

	"github.com/sentinel-official/sentinel-dvpnd/internal/leftover"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
)

var (
	ErrFirewallChanged       = errFirewallChanged
	ErrFirewallRowIncomplete = errFirewallRowIncomplete
)

func ApplyDNS(ctx context.Context, r *Runtime, iface string) error {
	placeholder := &connection{}

	r.mu.Lock()
	r.connection = placeholder
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.connection = nil
		r.mu.Unlock()
	}()

	return r.applyDNS(ctx, iface, placeholder)
}

func BlockFor(r *Runtime, terms session.Terms, settlement nodeapi.Settlement) (uint64, error) {
	return r.blockFor(terms, settlement)
}

func ClearConnection(r *Runtime) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.clearConnectionLocked()
}

func CloseChannels(ctx context.Context, r *Runtime) error { return r.closeChannels(ctx) }

func ConnectionHeartbeat(r *Runtime) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.connection.heartbeat
}

func Current(r *Runtime) (*Session, bool) { return r.current() }

func IssueErrorReason(err error) string { return issueErrorReason(err) }

func NewDarwinDNSBackend(kind, dir string, run runFunc) dnsBackend {
	return newDarwinDNSBackend(kind, dir, run)
}

func NewLinuxDNSBackend(kind, path, backupPath string) dnsBackend {
	return newLinuxDNSBackend(kind, path, backupPath)
}

func NewNetshBackend(dir string, run runFunc) dnsBackend {
	return newWindowsDNSBackend("netsh", dir, run)
}

func NewNetworksetupBackend(backupPath string, run runFunc) dnsBackend {
	return networksetupBackend{backupPath: backupPath, run: run}
}

func NewPFFirewall(anchor, tokenPath string, run runFunc) firewallBackend {
	return pfFirewall{anchor: anchor, run: run, tokenPath: tokenPath}
}

func NewResolvconfBackend(path, backupPath string) dnsBackend {
	return resolvconfBackend{
		backupPath: backupPath,
		path:       path,
	}
}

func NewWindowsDNSBackend(kind, dir string, run runFunc) dnsBackend {
	return newWindowsDNSBackend(kind, dir, run)
}

func ParseDNSServers(out []byte) []string { return parseDNSServers(out) }

func ParseNetworkServices(out []byte) []string { return parseNetworkServices(out) }

func ParsePFToken(out []byte) (string, error) { return parsePFToken(out) }

func PFLoaded(ctx context.Context, run runFunc, anchor string) (bool, error) {
	return pfLoaded(ctx, run, anchor)
}

func Poll(ctx context.Context, r *Runtime) error {
	r.mu.Lock()
	conn := r.connection
	r.mu.Unlock()
	conn.issueMu.Lock()
	defer conn.issueMu.Unlock()

	return r.poll(ctx, conn)
}

func ReadStream(
	ctx context.Context,
	r *Runtime,
	streamClient *nodeapi.ClientWithResponses,
	sessionID string,
) (bool, error) {
	conn := &connection{
		heartbeat:    time.Second,
		session:      Session{ID: sessionID},
		streamClient: streamClient,
	}

	return r.readStream(ctx, conn, nil)
}

func Recovered(r *Runtime) bool { return r.recovered.Load() }

func RunCommand(ctx context.Context, input, name string, args ...string) ([]byte, error) {
	return runCommand(ctx, input, name, args...)
}

func RunPFCommand(ctx context.Context, input, name string, args ...string) ([]byte, error) {
	return runPFCommand(ctx, input, name, args...)
}

func SampleAndIssue(ctx context.Context, r *Runtime) error {
	r.mu.Lock()
	conn := r.connection
	r.mu.Unlock()
	conn.issueMu.Lock()
	defer conn.issueMu.Unlock()

	return r.sampleAndIssue(ctx, conn, time.Now())
}

func SetDNSBackend(r *Runtime, backend dnsBackend) { r.dnsBackend = backend }

func SetFirewallBackend(r *Runtime, backend firewallBackend) { r.firewallBackend = backend }

func SetInterfaceRemover(r *Runtime, remover leftover.InterfaceRemover) { r.remover = remover }

func SetLookup(r *Runtime, lookup func(context.Context, string, string) ([]netip.Addr, error)) {
	r.lookup = lookup
}

func SetNodeClient(r *Runtime, nodeClient *nodeapi.ClientWithResponses) {
	r.mu.Lock()
	conn := r.connection
	r.mu.Unlock()
	conn.issueMu.Lock()
	defer conn.issueMu.Unlock()

	conn.nodeClient = nodeClient
}

func SetPendingTiming(r *Runtime, interval, timeout time.Duration) {
	r.pendingChannelInterval = interval
	r.pendingChannelTimeout = timeout
}

func SetService(r *Runtime, name service.Name, cfg any) { r.services[name] = cfg }

func SetTiming(r *Runtime, sample, poll time.Duration, attempts uint, backoff time.Duration) {
	r.sampleInterval = sample
	r.pollInterval = poll
	r.reconnectAttempts = attempts
	r.reconnectBackoff = backoff
}

func SettledBound(
	ctx context.Context,
	r *Runtime,
	channelAddress string,
	blockBaseUnits uint64,
	percent uint8,
) (uint64, error) {
	return r.settledBound(ctx, channelAddress, blockBaseUnits, percent)
}

func StartSession(ctx context.Context, r *Runtime, req *daemonv1.StartSessionRequest) (*Session, error) {
	return r.startSession(ctx, req)
}

func StopSession(ctx context.Context, r *Runtime) error {
	_, err := r.stopSession(ctx, reasonStoppedByUser)

	return err
}

func VerifyLedger(r *Runtime, ch *Channel, view nodeapi.Position, terms session.Terms, settledBound uint64) error {
	return r.verifyLedger(ch, view, terms, settledBound)
}

// SetWallClockOffset shifts the issue loop's wall clock by d (a system time change), leaving the
// monotonic clock alone.
func SetWallClockOffset(t interface{ Cleanup(func()) }, d time.Duration) {
	wallOffset.Store(int64(d))

	t.Cleanup(func() { wallOffset.Store(0) })
}
