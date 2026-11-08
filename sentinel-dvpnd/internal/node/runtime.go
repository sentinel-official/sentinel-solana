//go:build linux

package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/apiserver"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/leftover"
	"github.com/sentinel-official/sentinel-dvpnd/internal/worker"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-programs/clients/go/core"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	apiRateBurst       = 5
	apiRatePerSecond   = 1.0
	hnsdDirName        = "hnsd"
	hnsdPeers          = 8
	serviceStopTimeout = 10 * time.Second
)

const (
	checkTicketLeasesInterval = time.Hour
	redeemInterval            = time.Hour
	refreshMetadataInterval   = 20 * time.Minute
	runSpeedtestInterval      = 168 * time.Hour
	sendHeartbeatInterval     = 5 * time.Minute
	syncConfigInterval        = 5 * time.Minute
	syncPositionsInterval     = time.Minute
	syncUsageInterval         = time.Second
	updatePricesInterval      = 6 * time.Hour
)

var ErrNodeNotOpen = errors.New("node account is not open; run sentinel-dvpn node open-node with the owner key")

type Runtime struct {
	address         solana.PublicKey
	admitMu         sync.Mutex
	apiServer       *apiserver.Server
	cfg             *config.Node
	chainCfg        dvpn.Config
	chainNode       dvpn.Node
	closeMu         sync.Mutex
	db              *gorm.DB
	dial            dialFunc
	dir             string
	enforcers       map[string]session.Enforcer
	events          *eventHub
	iface           string
	ipLimiter       *api.Limiter
	log             zerolog.Logger
	metadata        *auth.SignedMessageCache[nodeapi.Metadata]
	meterMu         sync.Mutex
	metrics         metrics
	mu              sync.Mutex
	nonces          *ttlcache.Cache[auth.Nonce, struct{}]
	owner           solana.PublicKey
	placement       func(name service.Name) service.Placement
	privateKey      solana.PrivateKey
	programClient   *program.Client
	remover         leftover.InterfaceRemover
	sender          *tx.Sender
	serverCleanup   serverCleanupFunc
	service         service.Server
	signer          solana.PublicKey
	signerLimiter   *api.Limiter
	speed           nodeapi.Bandwidth
	voucherRequests map[string]voucherRequestState
}

func (r *Runtime) Jobs() []worker.Job {
	jobs := []worker.Job{
		{
			Interval: syncUsageInterval,
			Name:     "sync_usage",
			Run:      r.syncUsage,
		},
		{
			Interval: syncPositionsInterval,
			Name:     "sync_positions",
			Run:      r.syncPositions,
		},
		{
			Interval: sendHeartbeatInterval,
			Name:     "send_heartbeat",
			Run:      r.sendHeartbeat,
		},
		{
			Interval: syncConfigInterval,
			Name:     "sync_config",
			Run:      r.syncConfig,
		},
		{
			Interval: refreshMetadataInterval,
			Name:     "refresh_metadata",
			Run:      r.refreshMetadata,
		},
		{
			Interval: redeemInterval,
			Name:     "redeem",
			Run:      r.redeem,
		},
		{
			Interval: checkTicketLeasesInterval,
			Name:     "check_ticket_leases",
			Run:      r.checkTicketLeases,
		},
	}
	if r.cfg.Speedtest.Enabled {
		jobs = append(jobs, worker.Job{
			Interval: runSpeedtestInterval,
			Name:     "run_speedtest",
			Run:      r.runSpeedtest,
		})
	}

	if r.cfg.PriceFeed.Enabled {
		jobs = append(jobs, worker.Job{
			Interval: updatePricesInterval,
			Name:     "update_prices",
			Run:      r.updatePrices,
		})
	}

	return jobs
}

func (*Runtime) Models() []any {
	return []any{&Position{}, &Redemption{}, &Session{}, &Ticket{}}
}

func (r *Runtime) Register(registrar grpc.ServiceRegistrar) {
	daemonv1.RegisterNodeServiceServer(registrar, &grpcServer{runtime: r})
}

func (r *Runtime) Serve(ctx context.Context) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return r.apiServer.Serve(groupCtx) })
	group.Go(func() error { return r.waitService(groupCtx) })

	if r.cfg.HandshakeDNS.Enabled {
		group.Go(func() error {
			if err := runHNSD(groupCtx, filepath.Join(r.dir, hnsdDirName), hnsdPeers, r.log); err != nil {
				return fmt.Errorf("running hnsd: %w", err)
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return fmt.Errorf("serving node: %w", err)
	}

	return nil
}

func (r *Runtime) Start(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			r.nonces.Stop()
		}
	}()

	if err := r.syncChain(ctx); err != nil {
		return err
	}

	if err := r.clearLeftovers(ctx); err != nil {
		return err
	}

	if err := r.service.Start(ctx); err != nil {
		return fmt.Errorf("starting service: %w", err)
	}

	if err := r.reloadSessions(ctx); err != nil {
		r.stopService(ctx)

		return err
	}

	if err := r.refreshMetadata(ctx); err != nil {
		r.stopService(ctx)

		return err
	}

	r.registerRoutes()

	if err := r.sendHeartbeat(ctx); err != nil {
		r.log.Warn().Err(err).Msg("initial heartbeat failed")
	}

	return nil
}

func (r *Runtime) Stop(ctx context.Context) error {
	err := r.service.Stop(ctx)
	r.nonces.Stop()

	if r.ipLimiter != nil {
		r.ipLimiter.Stop()
	}

	if r.signerLimiter != nil {
		r.signerLimiter.Stop()
	}

	if err != nil {
		return fmt.Errorf("stopping service: %w", err)
	}

	return nil
}

func (r *Runtime) chainState() (dvpn.Config, dvpn.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.chainCfg, r.chainNode
}

func (r *Runtime) clearLeftovers(ctx context.Context) error {
	var (
		dirs []string
		errs []error
	)

	for _, name := range service.Names() {
		p := r.placement(name)
		dirs = append(dirs, p.ToolDir)

		if name == r.service.Name() {
			continue
		}

		err := r.serverCleanup(ctx, name, p)
		if err != nil && !errors.Is(err, service.ErrUnknownName) {
			errs = append(errs, fmt.Errorf("clearing %s leftovers: %w", name, err))
		}
	}

	errs = append(errs, leftover.RemoveCopies(dirs, r.iface), r.remover.RemoveInterface(ctx, r.iface))

	return errors.Join(errs...)
}

func (r *Runtime) registerRoutes() {
	r.ipLimiter = api.NewLimiter(apiRatePerSecond, apiRateBurst)
	r.signerLimiter = api.NewLimiter(apiRatePerSecond, apiRateBurst)
	nodeapi.RegisterRoutes(r.apiServer.Engine(), newHandler(r), r.nonces, r.ipLimiter, r.signerLimiter)
}

func (r *Runtime) stopService(ctx context.Context) {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), serviceStopTimeout)
	defer cancel()

	if err := r.service.Stop(stopCtx); err != nil {
		r.log.Warn().Err(err).Msg("stopping service after failed start")
	}
}

func (r *Runtime) syncChain(ctx context.Context) error {
	chainCfg, err := r.programClient.FetchConfigBySeeds(ctx)
	if err != nil {
		return fmt.Errorf("fetching config: %w", err)
	}

	account, err := r.programClient.FetchNode(ctx, r.address)
	if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
		return fmt.Errorf("%w: %s", ErrNodeNotOpen, r.address)
	}

	if err != nil {
		return fmt.Errorf("fetching node account %s: %w", r.address, err)
	}

	r.mu.Lock()
	r.chainCfg = chainCfg.Value
	r.chainNode = account.Value
	r.mu.Unlock()

	return nil
}

func (r *Runtime) waitService(ctx context.Context) error {
	err := r.service.Wait(ctx)
	switch {
	case err != nil && errors.Is(err, context.Canceled):
		return nil
	case err != nil:
		return fmt.Errorf("service exited: %w", err)
	case ctx.Err() != nil:
		return nil //nolint:nilerr
	default:
		return fmt.Errorf("service wait: %w", service.ErrProcessExited)
	}
}

type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

type serverCleanupFunc func(ctx context.Context, name service.Name, p service.Placement) error

type voucherRequestState struct {
	cumulativeBaseUnits uint64
	sentUnix            int64
}

func New(
	loaded *config.Loaded,
	db *gorm.DB,
	privateKey solana.PrivateKey,
	programClient *program.Client,
	sender *tx.Sender,
	log zerolog.Logger,
	reg prometheus.Registerer,
) (*Runtime, error) {
	owner, err := solana.PublicKeyFromBase58(loaded.Node.Owner)
	if err != nil {
		return nil, fmt.Errorf("parsing node.owner: %w", err)
	}

	id, err := parseID(loaded.Node.ID)
	if err != nil {
		return nil, fmt.Errorf("parsing node.id: %w", err)
	}

	address, _, err := dvpn.FindNodeAddress(programClient.ProgramID, owner, id)
	if err != nil {
		return nil, fmt.Errorf("deriving node address: %w", err)
	}

	metrics := newMetrics(reg)

	apiServer, err := apiserver.New(loaded.Dir, loaded.Node.API.Port, log, reg, nodeapi.StreamingPaths())
	if err != nil {
		return nil, fmt.Errorf("creating api server: %w", err)
	}

	srv, err := newService(loaded, log)
	if err != nil {
		return nil, err
	}

	r := &Runtime{
		address:         address,
		apiServer:       apiServer,
		cfg:             loaded.Node,
		db:              db,
		dial:            api.NewPeerDialer(net.DefaultResolver.LookupNetIP, loaded.File.Peer.AllowPrivate),
		dir:             loaded.Dir,
		enforcers:       map[string]session.Enforcer{},
		events:          newEventHub(),
		iface:           loaded.Tag.Interface(),
		log:             log,
		metrics:         metrics,
		nonces:          auth.NewNonceCache(auth.RequestNonceWindow),
		owner:           owner,
		placement:       loaded.Placement,
		privateKey:      privateKey,
		programClient:   programClient,
		remover:         leftover.NewRemover(),
		sender:          sender,
		serverCleanup:   service.CleanupServer,
		service:         srv,
		signer:          privateKey.PublicKey(),
		voucherRequests: map[string]voucherRequestState{},
	}
	r.metadata = auth.NewSignedMessageCache(r.buildMetadata, r.signMetadata)

	return r, nil
}

func newService(loaded *config.Loaded, log zerolog.Logger) (service.Server, error) {
	name, err := service.ParseName(loaded.Node.Service)
	if err != nil {
		return nil, fmt.Errorf("parsing node service: %w", err)
	}

	srv, err := service.NewServer(name, loaded.Services[name], loaded.Placement(name), log)
	if err != nil {
		return nil, fmt.Errorf("constructing %s service: %w", name, err)
	}

	return srv, nil
}

func parseID(raw string) ([32]uint8, error) {
	return program.ParseID(raw)
}
