package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/leftover"
	"github.com/sentinel-official/sentinel-dvpnd/internal/rpcpin"
	"github.com/sentinel-official/sentinel-dvpnd/internal/worker"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
)

const (
	callTimeout           = 15 * time.Second
	closeChannelsInterval = 10 * time.Minute
)

type Runtime struct {
	callTimeout            time.Duration
	cfg                    *config.Client
	connection             *connection
	db                     *gorm.DB
	dnsBackend             dnsBackend
	dnsIface               string
	firewallBackend        firewallBackend
	firewallState          firewallState
	iface                  string
	log                    zerolog.Logger
	lookup                 func(context.Context, string, string) ([]netip.Addr, error)
	metrics                metrics
	mu                     sync.Mutex
	nodeHosts              nodeHosts
	owner                  solana.PublicKey
	peerCfg                cli.PeerConfig
	pendingChannelInterval time.Duration
	pendingChannelTimeout  time.Duration
	pin                    *rpcpin.Pin
	placement              func(name service.Name) service.Placement
	pollInterval           time.Duration
	privateKey             solana.PrivateKey
	programClient          *program.Client
	reconnectAttempts      uint
	reconnectBackoff       time.Duration
	recovered              atomic.Bool
	remover                leftover.InterfaceRemover
	rpcCfg                 cli.RPCConfig
	sampleInterval         time.Duration
	sender                 *tx.Sender
	services               map[service.Name]any
	signer                 solana.PublicKey
	snapshot               atomic.Pointer[Session]
	stopping               bool
	turns                  turnQueue
	watchers               map[*watcher]struct{}
}

func (r *Runtime) Jobs() []worker.Job {
	return []worker.Job{
		{
			Interval: closeChannelsInterval,
			Name:     "close_channels",
			Run:      r.closeChannels,
		},
	}
}

func (r *Runtime) Models() []any {
	return []any{&Channel{}, &Firewall{}, &Session{}, &Ticket{}, &Voucher{}}
}

func (r *Runtime) Register(s grpc.ServiceRegistrar) {
	daemonv1.RegisterClientServiceServer(s, &grpcServer{runtime: r})
}

func (r *Runtime) Serve(ctx context.Context) error {
	<-ctx.Done()

	return nil
}

func (r *Runtime) Start(ctx context.Context) error { return r.resolvePendingChannels(ctx) }

func (r *Runtime) Stop(ctx context.Context) error {
	_, err := r.stopSession(ctx, reasonDaemonStopping)
	if errors.Is(err, errMissingSession) {
		return nil
	}

	return err
}

func (r *Runtime) findFirst(ctx context.Context, dest any, conds ...any) error {
	ctx, cancel := r.withTimeout(ctx)
	defer cancel()

	return r.db.WithContext(ctx).First(dest, conds...).Error
}

func (r *Runtime) save(ctx context.Context, value any) error {
	ctx, cancel := r.withTimeout(ctx)
	defer cancel()

	return r.db.WithContext(ctx).Save(value).Error
}

func (r *Runtime) saveEndedSession(ctx context.Context, s *Session) {
	ctx, cancel := r.withDetachedTimeout(ctx)
	defer cancel()

	if err := r.db.WithContext(ctx).Save(s).Error; err != nil {
		r.log.Error().Err(err).Str("session", s.ID).Msg("recording the ended session")
	}
}

func (r *Runtime) withDetachedTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), r.callTimeout)
}

func (r *Runtime) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, r.callTimeout)
}

type runFunc func(ctx context.Context, input, name string, args ...string) ([]byte, error)

type turn struct {
	done chan struct{}
	prev chan struct{}
}

func (t turn) run(op func()) {
	defer close(t.done)

	if t.prev != nil {
		<-t.prev
	}

	op()
}

type turnQueue struct {
	mu   sync.Mutex
	tail chan struct{}
}

func (q *turnQueue) enqueue() turn {
	q.mu.Lock()
	defer q.mu.Unlock()

	t := turn{
		done: make(chan struct{}),
		prev: q.tail,
	}
	q.tail = t.done

	return t
}

func New(
	loaded *config.Loaded,
	db *gorm.DB,
	privateKey solana.PrivateKey,
	programClient *program.Client,
	sender *tx.Sender,
	log zerolog.Logger,
	reg prometheus.Registerer,
	pin *rpcpin.Pin,
) (*Runtime, error) {
	owner, err := solana.PublicKeyFromBase58(loaded.Client.Owner)
	if err != nil {
		return nil, fmt.Errorf("parsing client.owner: %w", err)
	}

	return &Runtime{
		callTimeout:            callTimeout,
		cfg:                    loaded.Client,
		db:                     db,
		dnsBackend:             newDNSBackend(loaded.Client.Network.DNSBackend, loaded.Dir),
		firewallBackend:        newFirewall(loaded.Dir, loaded.Tag.ObjectName()),
		iface:                  loaded.Tag.Interface(),
		log:                    log,
		lookup:                 net.DefaultResolver.LookupNetIP,
		metrics:                newMetrics(reg),
		owner:                  owner,
		peerCfg:                loaded.File.Peer,
		pendingChannelInterval: pendingChannelInterval,
		pendingChannelTimeout:  pendingChannelTimeout,
		pin:                    pin,
		placement:              loaded.Placement,
		pollInterval:           pollInterval,
		privateKey:             privateKey,
		programClient:          programClient,
		reconnectAttempts:      reconnectAttempts,
		reconnectBackoff:       reconnectBackoff,
		remover:                leftover.NewRemover(),
		rpcCfg:                 loaded.File.RPC,
		sampleInterval:         sampleInterval,
		sender:                 sender,
		services:               loaded.Services,
		signer:                 privateKey.PublicKey(),
		watchers:               map[*watcher]struct{}{},
	}, nil
}

func runCommand(ctx context.Context, input, name string, args ...string) ([]byte, error) {
	path, err := service.LookPath(name)
	if err != nil {
		return nil, err
	}

	cmd := service.CommandContext(ctx, path, args...)
	if input != "" {
		cmd.Stdin = strings.NewReader(input)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("running %s: %w", name, err)
	}

	return output, nil
}
