//go:build linux

package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc/jsonrpc"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/apiserver"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/worker"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/providerapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-programs/clients/go/core"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	apiRateBurst     = 5
	apiRatePerSecond = 1
)

const (
	keepLeasesInterval      = 5 * time.Minute
	probeNodesInterval      = 5 * time.Minute
	refreshMetadataInterval = 20 * time.Minute
	syncConfigInterval      = 5 * time.Minute
)

var ErrProviderNotOpen = errors.New("provider is not open; run `sentinel-dvpn provider open-provider`")

type Runtime struct {
	address       solana.PublicKey
	apiServer     *apiserver.Server
	cfg           *config.Provider
	chainCfg      dvpn.Config
	closeMu       sync.Mutex
	db            *gorm.DB
	dial          dialFunc
	dir           string
	keepMu        sync.Mutex
	limiter       *api.Limiter
	log           zerolog.Logger
	metadata      *auth.SignedMessageCache[providerapi.Metadata]
	metrics       metrics
	mu            sync.Mutex
	nonces        *ttlcache.Cache[auth.Nonce, struct{}]
	owner         solana.PublicKey
	privateKey    solana.PrivateKey
	programClient *program.Client
	sender        *tx.Sender
	signer        solana.PublicKey
	stopping      bool
	watchers      map[string]*leaseWatcher
}

func (r *Runtime) Jobs() []worker.Job {
	return []worker.Job{
		{
			Interval: probeNodesInterval,
			Name:     "probe_nodes",
			Run:      r.probeNodes,
		},
		{
			Interval: keepLeasesInterval,
			Name:     "keep_leases",
			Run:      r.keepLeases,
		},
		{
			Interval: refreshMetadataInterval,
			Name:     "refresh_metadata",
			Run:      r.refreshMetadata,
		},
		{
			Interval: syncConfigInterval,
			Name:     "sync_config",
			Run:      r.syncConfig,
		},
	}
}

func (r *Runtime) Models() []any {
	return []any{&Lease{}, &Plan{}, &Probe{}, &RosterNode{}, &Subscription{}, &Ticket{}}
}

func (r *Runtime) Register(registrar grpc.ServiceRegistrar) {
	daemonv1.RegisterProviderServiceServer(registrar, &grpcServer{runtime: r})
}

func (r *Runtime) Serve(ctx context.Context) error { return r.apiServer.Serve(ctx) }

func (r *Runtime) Start(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			r.limiter.Stop()
			r.nonces.Stop()
		}
	}()

	if _, err := r.syncChainAtStart(ctx); err != nil {
		return err
	}

	if err := r.metadata.Refresh(ctx); err != nil {
		return fmt.Errorf("refreshing metadata: %w", err)
	}

	if err := r.db.WithContext(ctx).Model(&Probe{}).
		Where("probed_unix <> 0").Update("probed_unix", 0).Error; err != nil {
		return fmt.Errorf("invalidating cached node probes: %w", err)
	}

	providerapi.RegisterRoutes(r.apiServer.Engine(), &handler{runtime: r}, r.limiter)

	return nil
}

func (r *Runtime) Stop(_ context.Context) error {
	r.stopLeaseWatchers()
	r.limiter.Stop()
	r.nonces.Stop()

	return nil
}

func (r *Runtime) chainConfig() dvpn.Config {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.chainCfg
}

// startChainRetryFirst, startChainRetryMax: the waits between the start-up chain reads while the
// RPC answers with a rate limit or a server error. A busy RPC at start is waited out, not fatal.
var (
	startChainRetryFirst = time.Second
	startChainRetryMax   = 30 * time.Second
)

// jsonRPCRateLimited: the JSON-RPC rate-limit code RPC providers and gateways send inside an HTTP 200.
const jsonRPCRateLimited = -32429

// rpcBusy: the RPC answered with a rate limit or a server error (HTTP or JSON-RPC 429, -32429, 5xx).
// The request was fine; the same read succeeds once the RPC has room again.
func rpcBusy(err error) bool {
	var rpcErr *jsonrpc.RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.Code == http.StatusTooManyRequests || rpcErr.Code == jsonRPCRateLimited ||
			rpcErr.Code >= http.StatusInternalServerError
	}

	var httpErr *jsonrpc.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Code == http.StatusTooManyRequests || httpErr.Code >= http.StatusInternalServerError
	}

	return false
}

// syncChainAtStart: the first chain read of Start, retried while the RPC is busy. Nothing has been
// started yet, so a retry leaves no state behind; any other error stops Start as before.
func (r *Runtime) syncChainAtStart(ctx context.Context) (dvpn.Provider, error) {
	delay, attempts, started := startChainRetryFirst, 1, time.Now()

	for {
		provider, err := r.syncChain(ctx)
		if err == nil && attempts > 1 {
			r.log.Info().Int("attempts", attempts).Dur("waited", time.Since(started)).
				Msg("the chain RPC answered; starting")
		}

		if err == nil || !rpcBusy(err) {
			return provider, err
		}

		r.log.Warn().Err(err).Dur("retry_in", delay).Msg("the chain RPC is busy; waiting for it before serving")

		select {
		case <-ctx.Done():
			return dvpn.Provider{}, fmt.Errorf("waiting for the rpc: %w", errors.Join(ctx.Err(), err))
		case <-time.After(delay):
		}

		attempts++
		delay = min(2*delay, startChainRetryMax)
	}
}

func (r *Runtime) syncChain(ctx context.Context) (dvpn.Provider, error) {
	cfg, err := r.programClient.FetchConfigBySeeds(ctx)
	if err != nil {
		return dvpn.Provider{}, fmt.Errorf("fetching program config: fetching config: %w", err)
	}

	providerAccount, err := r.programClient.FetchProvider(ctx, r.address)
	if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
		return dvpn.Provider{}, fmt.Errorf("%w: %s", ErrProviderNotOpen, r.address)
	}

	if err != nil {
		return dvpn.Provider{}, fmt.Errorf("fetching provider account: %w", err)
	}

	r.mu.Lock()
	r.chainCfg = cfg.Value
	r.mu.Unlock()

	return providerAccount.Value, nil
}

func (r *Runtime) syncConfig(ctx context.Context) error {
	provider, err := r.syncChain(ctx)
	if err != nil {
		return err
	}

	uri := apiURL(r.cfg.API.Hosts[0], r.cfg.API.Port)
	if program.ProviderURI(provider) == uri {
		return nil
	}

	grant, err := r.programClient.GrantAddress(ctx, r.owner, r.signer, dvpn.PermUpdateProvider, r.address)
	if err != nil {
		return err
	}

	body := []byte(uri)

	ix, err := dvpn.BuildUpdateProviderInstruction(r.programClient.ProgramID, dvpn.UpdateProviderParams{
		Grant:    grant,
		Provider: r.address,
		Signer:   r.signer,
		URI:      &body,
	})
	if err != nil {
		return err
	}

	_, err = r.sender.Send(ctx, r.privateKey, []solana.Instruction{ix}, tx.WithLabel("update_provider"))

	return err
}

type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

func New(
	loaded *config.Loaded,
	db *gorm.DB,
	privateKey solana.PrivateKey,
	programClient *program.Client,
	sender *tx.Sender,
	log zerolog.Logger,
	reg prometheus.Registerer,
) (*Runtime, error) {
	owner, err := solana.PublicKeyFromBase58(loaded.Provider.Owner)
	if err != nil {
		return nil, fmt.Errorf("parsing provider.owner: %w", err)
	}

	id, err := parseID(loaded.Provider.ID)
	if err != nil {
		return nil, fmt.Errorf("parsing provider.id: %w", err)
	}

	address, _, err := dvpn.FindProviderAddress(programClient.ProgramID, owner, id)
	if err != nil {
		return nil, fmt.Errorf("deriving provider address: %w", err)
	}

	apiServer, err := apiserver.New(loaded.Dir, loaded.Provider.API.Port, log, reg, nil)
	if err != nil {
		return nil, fmt.Errorf("creating api server: %w", err)
	}

	r := &Runtime{
		address:       address,
		apiServer:     apiServer,
		cfg:           loaded.Provider,
		db:            db,
		dial:          api.NewPeerDialer(net.DefaultResolver.LookupNetIP, loaded.File.Peer.AllowPrivate),
		dir:           loaded.Dir,
		limiter:       api.NewLimiter(apiRatePerSecond, apiRateBurst),
		log:           log,
		metrics:       newMetrics(reg),
		nonces:        auth.NewNonceCache(auth.RequestNonceWindow),
		owner:         owner,
		privateKey:    privateKey,
		programClient: programClient,
		sender:        sender,
		signer:        privateKey.PublicKey(),
		watchers:      make(map[string]*leaseWatcher),
	}
	r.metadata = auth.NewSignedMessageCache(
		r.buildMetadata,
		func(metadata providerapi.Metadata) (auth.SignedMessage, error) {
			return providerapi.SignMetadata(privateKey, metadata)
		},
	)

	return r, nil
}

func parseID(raw string) ([32]uint8, error) { return program.ParseID(raw) }
