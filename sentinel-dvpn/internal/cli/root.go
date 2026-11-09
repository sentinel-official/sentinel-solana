package cli

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/spf13/cobra"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sentinel-official/sentinel-dvpn/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/paymaster"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	keyCmdName      = "key"
	redactedValue   = "[redacted]"
	unlimitedAmount = "unlimited"
)

//nolint:gochecknoglobals
var (
	errConfigExists  = errors.New("config file already exists")
	errInvalidAmount = errors.New("amount must be a non-negative integer or \"unlimited\"")
	errUnknownMode   = errors.New("unknown mode")
	modeByName       = map[string]uint32{
		"stream":  uint32(dvpn.PositionModeStream),
		"voucher": uint32(dvpn.PositionModeVoucher),
	}
)

func NewRootCmd() *cobra.Command {
	home := cli.DefaultHome("." + instance.ProductName)
	cfg := &config.File{}
	daemon := &dialer{}
	signer := &signerKey{}
	printer := &Printer{}
	programClient := &program.Client{}
	sender := &tx.Sender{}
	runner := newRunner(programClient, sender, signer, printer)

	root := &cobra.Command{
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return preRun(cmd, home, cfg, programClient, sender, signer, daemon, printer)
		},
		Short: "sentinel dvpn command tool",
		Use:   instance.ProductName,
	}

	root.PersistentFlags().StringVar(&home, "home", home, "home directory")
	config.AddFlags(root.PersistentFlags())
	root.AddCommand(
		cli.NewCompletionCmd("dvpn"),
		cli.NewKeyCmd(func() string { return config.KeysDir(home) }),
		cli.NewVersionCmd(),
		newConfigCmd(cfg, printer, &home),
		newDaemonCmd(daemon, printer),
		newClientCmd(daemon, printer),
		newTicketCmd(programClient, signer, cfg, printer),
		newWalletCmd(programClient, sender, signer, printer),
	)

	for _, group := range newChainGroups(runner) {
		root.AddCommand(group)
	}

	addQueryCmds(root, &programClient.Client, printer)
	addNodeInspectCmd(root, programClient, signer, cfg, printer)
	addNodeDaemonCmds(root, daemon, printer)
	addProviderDaemonCmds(root, daemon, printer)

	return root
}

func configOptional(cmd *cobra.Command) bool {
	for current := cmd; current != nil; current = current.Parent() {
		switch current.Name() {
		case keyCmdName, "completion", "version", cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd:
			return true
		case "init":
			if parent := current.Parent(); parent != nil && parent.Name() == "config" {
				return true
			}
		}
	}

	return false
}

func newConfigCmd(cfg *config.File, printer *Printer, home *string) *cobra.Command {
	cmd := &cobra.Command{
		Short: "manage the cli configuration file",
		Use:   "config",
	}
	cmd.AddCommand(newConfigInitCmd(printer, home), newConfigShowCmd(cfg, printer))

	return cmd
}

func newConfigInitCmd(printer *Printer, home *string) *cobra.Command {
	var force bool

	run := func(_ *cobra.Command, _ []string) error {
		path := filepath.Join(*home, config.FileName)

		if !force {
			_, err := os.Stat(path)
			if err == nil {
				return fmt.Errorf("%w: %s", errConfigExists, path)
			}

			if !os.IsNotExist(err) {
				return fmt.Errorf("checking %s: %w", path, err)
			}
		}

		file := config.DefaultFile()
		if err := config.Save(*home, file); err != nil {
			return err
		}

		return printer.Print(file)
	}

	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		RunE:  run,
		Short: "save the default configuration into the home directory",
		Use:   "init",
	}
	cmd.Flags().BoolVar(&force, "force", false, "overwrite an existing config file")

	return cmd
}

func newConfigShowCmd(cfg *config.File, printer *Printer) *cobra.Command {
	run := func(_ *cobra.Command, _ []string) error {
		shown := *cfg
		if shown.Tx.Paymaster.APIKey != "" {
			shown.Tx.Paymaster.APIKey = redactedValue
		}

		return printer.Print(&shown)
	}

	return &cobra.Command{
		Args:  cobra.NoArgs,
		RunE:  run,
		Short: "print the loaded configuration",
		Use:   "show",
	}
}

func newDaemonCmd(daemon *dialer, printer *Printer) *cobra.Command {
	cmd := &cobra.Command{
		Short: "talk to the sentinel-dvpnd daemon over its control endpoint",
		Use:   "daemon",
	}
	cmd.AddCommand(newDaemonStatusCmd(daemon, printer), newDaemonVersionCmd(daemon, printer))

	return cmd
}

func newDaemonStatusCmd(daemon *dialer, printer *Printer) *cobra.Command {
	spec := callSpec[healthv1.HealthCheckResponse]{
		Args:   cobra.NoArgs,
		Render: func(resp *healthv1.HealthCheckResponse) any { return resp.GetStatus().String() },
		Short:  "report whether the daemon is serving",
		Use:    "status",
	}

	return newUnaryCallCmd(daemon, printer, spec, healthv1.NewHealthClient, healthv1.HealthClient.Check, nil)
}

func newDaemonVersionCmd(daemon *dialer, printer *Printer) *cobra.Command {
	spec := callSpec[daemonv1.GetVersionResponse]{
		Args:   cobra.NoArgs,
		Render: func(resp *daemonv1.GetVersionResponse) any { return resp.GetVersion() },
		Short:  "print the daemon's build identity",
		Use:    "version",
	}

	return newUnaryCallCmd(
		daemon,
		printer,
		spec,
		daemonv1.NewDaemonServiceClient,
		daemonv1.DaemonServiceClient.GetVersion,
		nil,
	)
}

func parseAddress(label, raw string) (solana.PublicKey, error) {
	address, err := solana.PublicKeyFromBase58(raw)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("parsing %s address %q: %w", label, raw, err)
	}

	return address, nil
}

func parseApproveAmount(amount string) (uint64, error) {
	if amount == unlimitedAmount {
		return math.MaxUint64, nil
	}

	baseUnits, err := strconv.ParseUint(amount, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s", errInvalidAmount, amount)
	}

	return baseUnits, nil
}

func parseMode(name string) (uint32, error) {
	mode, ok := modeByName[name]
	if !ok {
		return 0, fmt.Errorf("%w: %s", errUnknownMode, name)
	}

	return mode, nil
}

func preRun(
	cmd *cobra.Command,
	home string,
	cfg *config.File,
	programClient *program.Client,
	sender *tx.Sender,
	signer *signerKey,
	daemon *dialer,
	printer *Printer,
) error {
	file, err := config.Load(home, cmd.Root().PersistentFlags())
	if err != nil {
		if !configOptional(cmd) {
			return err
		}

		defaults := config.DefaultFile()
		file = &defaults
	}

	*cfg = *file
	*daemon = dialer{
		endpoint:  cfg.Daemon.Endpoint,
		stateRoot: instance.StateRoot(runtime.GOOS),
	}
	*signer = signerKey{
		dir:  config.KeysDir(home),
		name: cfg.Key.Name,
	}

	*printer = *NewPrinter(cmd.OutOrStdout(), cfg.Output)
	log := cli.NewLogger(cmd.ErrOrStderr(), cfg.Log)
	daemon.log = log

	if err := warnDaemonEnv(log); err != nil {
		return err
	}

	if configOptional(cmd) {
		return nil
	}

	built, err := program.NewClientFromBuild(cfg.RPC.NewClient(), cfg.RPC.Commitment)
	if err != nil {
		return err
	}

	*programClient = *built
	*sender = *tx.NewSender(programClient.RPC, cfg.RPC.DialWS, cfg.Tx, log)
	sender.ProgramIDs = tx.ProgramIDs{DVPN: programClient.ProgramID}

	if cfg.Tx.Paymaster.URL == "" || !isSending(cmd) {
		return nil
	}

	kora, err := paymaster.NewKora(programClient.RPC, cfg.Tx.Paymaster, programClient.Mint)
	if err != nil {
		return fmt.Errorf("building paymaster: %w", err)
	}

	sender.Paymaster = kora

	return nil
}

func warnDaemonEnv(log zerolog.Logger) error {
	mismatches, err := config.DetectDaemonEnv(os.Environ())
	if err != nil {
		return fmt.Errorf("detecting daemon variables: %w", err)
	}

	for _, m := range mismatches {
		log.Warn().Str("expected", m.Expected).Str("variable", m.Actual).Msg("ignoring a sentinel-dvpnd variable")
	}

	return nil
}
