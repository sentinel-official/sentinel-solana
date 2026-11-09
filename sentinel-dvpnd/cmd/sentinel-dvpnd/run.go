package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/daemon"
	"github.com/sentinel-official/sentinel-dvpnd/internal/preflight"
	"github.com/sentinel-official/sentinel-dvpnd/internal/rpcpin"
	"github.com/sentinel-official/sentinel-dvpnd/internal/store"
	"github.com/sentinel-official/sentinel-dvpnd/internal/worker"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/key"
	"github.com/sentinel-official/sentinel-go-sdk/v3/paymaster"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
)

const (
	storeFileName = "data.db"

	// A client that starts without the RPC retries its startup checks after 5 s, doubling up to once a minute.
	startupRetryFirst = 5 * time.Second
	startupRetryMax   = time.Minute
)

var errWebSocketUnreachable = errors.New("connecting rpc")

var serverRuntimes = map[string]runtimeFactory{} //nolint:gochecknoglobals

type runtimeFactory func(
	loaded *config.Loaded,
	db *gorm.DB,
	privateKey solana.PrivateKey,
	programClient *program.Client,
	sender *tx.Sender,
	log zerolog.Logger,
	reg prometheus.Registerer,
) (daemon.Runtime, error)

func checkWebSocket(ctx context.Context, loaded *config.Loaded, pin *rpcpin.Pin) error {
	if pin != nil {
		if _, active := pin.Pinned(); active {
			return nil
		}
	}

	conn, err := loaded.File.RPC.DialWS(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", errWebSocketUnreachable, err)
	}

	conn.Close()

	return nil
}

func closeStore(db *gorm.DB, log zerolog.Logger) {
	sqlDB, err := db.DB()
	if err != nil {
		log.Warn().Err(err).Msg("closing store")

		return
	}

	if err := sqlDB.Close(); err != nil {
		log.Warn().Err(err).Msg("closing store")
	}
}

func loadPrivateKey(loaded *config.Loaded) (solana.PrivateKey, error) {
	path, err := loaded.KeyPath()
	if err != nil {
		return nil, err
	}

	passphrase, err := loaded.LoadPassphrase()
	if err != nil {
		return nil, err
	}

	privateKey, err := key.Load(path, passphrase)
	if errors.Is(err, key.ErrKeyLocked) {
		return nil, fmt.Errorf("%w: set passphrase_file or %s", err, config.PassphraseEnv)
	}

	if err != nil {
		return nil, fmt.Errorf("loading key: %w", err)
	}

	return privateKey, nil
}

func newRPCClient(loaded *config.Loaded) (*rpcpin.Pin, *rpc.Client, error) {
	if loaded.Role != config.RoleClient {
		return nil, loaded.File.RPC.NewClient(), nil
	}

	pin, err := rpcpin.New(loaded.File.RPC.URL, loaded.File.RPC.Dialer())
	if err != nil {
		return nil, nil, err
	}

	return pin, loaded.File.RPC.NewClient(api.WithDialContext(pin.DialContext)), nil
}

func newRunCmd(opts *instanceOptions) *cobra.Command {
	cmd := &cobra.Command{
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDaemon(cmd.Context(), opts, cmd.Flags())
		},
		Short: "run the daemon in the foreground",
		Use:   "run",
	}
	config.AddFlags(cmd.Flags())

	return cmd
}

func newRuntime(
	loaded *config.Loaded,
	db *gorm.DB,
	privateKey solana.PrivateKey,
	programClient *program.Client,
	sender *tx.Sender,
	log zerolog.Logger,
	reg prometheus.Registerer,
	pin *rpcpin.Pin,
) (daemon.Runtime, error) {
	if loaded.Role == config.RoleClient {
		return client.New(loaded, db, privateKey, programClient, sender, log, reg, pin)
	}

	factory, ok := serverRuntimes[loaded.Role]
	if !ok {
		return nil, fmt.Errorf("%w: %s", config.ErrUnsupportedRole, loaded.Role)
	}

	return factory(loaded, db, privateKey, programClient, sender, log, reg)
}

func prepareRuntime(
	ctx context.Context,
	loaded *config.Loaded,
	rt daemon.Runtime,
	rpcClient *rpc.Client,
	pin *rpcpin.Pin,
	log zerolog.Logger,
) (func(context.Context) error, error) {
	if recoverer, ok := rt.(daemon.Recoverer); ok {
		if err := recoverer.Recover(ctx); err != nil {
			if errors.Is(err, client.ErrDNSCleanupFailed) {
				return nil, fmt.Errorf("recovering dns: %w", err)
			}

			if errors.Is(err, client.ErrServiceCleanupFailed) {
				return nil, fmt.Errorf("recovering services: %w", err)
			}

			log.Warn().Err(err).Msg("recovery could not verify the persisted kill switch; StopSession releases it")
		}
	}

	err := checkChainAccess(ctx, loaded, rpcClient, pin, log)
	if err == nil {
		return nil, nil
	}

	// Only the client waits for the network: it has to keep serving its own control API (StopSession is what
	// releases a kill switch held from an earlier session), and a boot without network yet is normal for it.
	if loaded.Role != config.RoleClient || !chainUnreachable(err) {
		return nil, err
	}

	log.Warn().Err(err).Msg("rpc unreachable at start; serving and waiting for the network")

	return func(ctx context.Context) error {
		return waitForChainAccess(ctx, loaded, rpcClient, pin, log, startupRetryFirst, startupRetryMax)
	}, nil
}

// checkChainAccess runs the preflight and the websocket check that every start needs.
func checkChainAccess(
	ctx context.Context,
	loaded *config.Loaded,
	rpcClient *rpc.Client,
	pin *rpcpin.Pin,
	log zerolog.Logger,
) error {
	if _, err := preflight.Run(ctx, loaded, log, rpcClient); err != nil {
		return err
	}

	return checkWebSocket(ctx, loaded, pin)
}

// chainUnreachable reports a startup failure caused only by the network: the RPC did not answer, or its
// websocket could not be dialled. Anything else (configuration, identity, privileges, mint) is not.
func chainUnreachable(err error) bool {
	return errors.Is(err, preflight.ErrRPCUnreachable) || errors.Is(err, errWebSocketUnreachable)
}

// waitForChainAccess retries the startup checks until they pass. It returns nil once they pass or when ctx ends,
// and an error, which stops the daemon as a failed start would, when the RPC answers but a check still fails.
func waitForChainAccess(
	ctx context.Context,
	loaded *config.Loaded,
	rpcClient *rpc.Client,
	pin *rpcpin.Pin,
	log zerolog.Logger,
	first, maxWait time.Duration,
) error {
	wait := first

	for {
		timer := time.NewTimer(wait)

		select {
		case <-ctx.Done():
			timer.Stop()

			return nil
		case <-timer.C:
		}

		err := checkChainAccess(ctx, loaded, rpcClient, pin, log)

		switch {
		case err == nil:
			log.Info().Msg("rpc reachable; startup checks passed")

			return nil
		case ctx.Err() != nil:
			return nil
		case !chainUnreachable(err):
			return fmt.Errorf("startup checks once the rpc answered: %w", err)
		}

		wait = min(wait*2, maxWait)
		log.Warn().Err(err).Dur("retry_in", wait).Msg("rpc still unreachable")
	}
}

func runInstance(
	ctx context.Context,
	opts *instanceOptions,
	flags *pflag.FlagSet,
	out io.Writer,
	ready daemon.Ready,
) error {
	loaded, err := opts.load(flags)
	if err != nil {
		return err
	}

	release, err := daemon.Lock(loaded.Dir)
	if err != nil {
		return fmt.Errorf("locking instance folder: %w", err)
	}
	defer func() { _ = release() }()

	log := cli.NewLogger(out, loaded.File.Log).With().Str("role", loaded.Role).Logger()
	if err := warnCLIEnv(log); err != nil {
		return err
	}

	privateKey, err := loadPrivateKey(loaded)
	if err != nil {
		return err
	}

	return runRuntime(ctx, loaded, privateKey, log, ready)
}

func runRuntime(
	ctx context.Context,
	loaded *config.Loaded,
	privateKey solana.PrivateKey,
	log zerolog.Logger,
	ready daemon.Ready,
) error {
	pin, rpcClient, err := newRPCClient(loaded)
	if err != nil {
		return err
	}

	programClient, err := program.NewClientFromBuild(rpcClient, loaded.File.RPC.Commitment)
	if err != nil {
		return err
	}

	sender := tx.NewSender(
		rpcClient,
		rpcpin.NewWSDialer(loaded.File.RPC.WSURL, loaded.File.RPC.Timeout, pin, log),
		loaded.File.Tx,
		log,
	)
	sender.ProgramIDs = tx.ProgramIDs{DVPN: programClient.ProgramID}

	if cfg := loaded.File.Tx.Paymaster; cfg.URL != "" {
		kora, err := paymaster.NewKora(rpcClient, cfg, programClient.Mint)
		if err != nil {
			return fmt.Errorf("building paymaster: %w", err)
		}

		sender.Paymaster = kora
	}

	db, err := store.Open(filepath.Join(loaded.Dir, storeFileName))
	if err != nil {
		return fmt.Errorf("opening store: %w", err)
	}
	defer closeStore(db, log)

	reg := daemon.NewRegistry()
	roleReg := prometheus.WrapRegistererWith(prometheus.Labels{"role": loaded.Role}, reg)

	rt, err := newRuntime(loaded, db, privateKey, programClient, sender, log, roleReg, pin)
	if err != nil {
		return fmt.Errorf("building %s runtime: %w", loaded.Role, err)
	}

	if err := store.Migrate(ctx, db, rt.Models()...); err != nil {
		return fmt.Errorf("migrating store: %w", err)
	}

	startup, err := prepareRuntime(ctx, loaded, rt, rpcClient, pin, log)
	if err != nil {
		return err
	}

	return serve(ctx, loaded, rt, reg, roleReg, log, ready, startup)
}

func serve(
	ctx context.Context,
	loaded *config.Loaded,
	rt daemon.Runtime,
	reg *prometheus.Registry,
	roleReg prometheus.Registerer,
	log zerolog.Logger,
	ready daemon.Ready,
	startup func(context.Context) error,
) error {
	listener, err := daemon.Listen(ctx, loaded.Endpoint(), config.AccessGroup(loaded.Tag, loaded.Dir))
	if err != nil {
		return fmt.Errorf("listening: %w", err)
	}
	defer func() { _ = listener.Close() }()

	server := daemon.NewServer(log, reg)

	var aux []func(context.Context) error

	if startup != nil {
		aux = append(aux, startup)
	}

	if loaded.File.Metrics.Enabled {
		var listenConfig net.ListenConfig

		metricsListener, err := listenConfig.Listen(ctx, "tcp", loaded.File.Metrics.Listen)
		if err != nil {
			return fmt.Errorf("listening on metrics: %w", err)
		}

		defer func() { _ = metricsListener.Close() }()

		aux = append(aux, func(ctx context.Context) error {
			return daemon.ServeMetrics(ctx, metricsListener, reg)
		})
	}

	runner, err := worker.New(log, roleReg)
	if err != nil {
		return err
	}

	return daemon.Run(ctx, rt, server, listener, runner, log, ready, aux...)
}

func warnCLIEnv(log zerolog.Logger) error {
	mismatches, err := config.DetectCLIEnv(os.Environ())
	if err != nil {
		return fmt.Errorf("detecting cli variables: %w", err)
	}

	for _, m := range mismatches {
		log.Warn().Str("expected", m.Expected).Str("variable", m.Actual).Msg("ignoring a sentinel-dvpn cli variable")
	}

	return nil
}
