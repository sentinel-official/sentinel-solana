package main

import (
	"context"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/spf13/cobra"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/daemon"
	"github.com/sentinel-official/sentinel-dvpnd/internal/rpcpin"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
)

const (
	ServiceCommandIgnore      = serviceCommandIgnore
	ServiceCommandInterrogate = serviceCommandInterrogate
	ServiceCommandStop        = serviceCommandStop
	ServiceStateRunning       = serviceStateRunning
	ServiceStateStarting      = serviceStateStarting
	ServiceStateStopping      = serviceStateStopping
)

var (
	ErrInstanceExists    = errInstanceExists
	ErrInvalidBinaryPath = errInvalidBinaryPath
	ErrInvalidDir        = errInvalidDir
	ErrMissingInstance   = errMissingInstance
)

type ExecFunc = execFunc

type InitResult = initResult

type SCMRegistration = scmRegistration

func NewSCMRegistration(tag instance.Tag) SCMRegistration { return newSCMRegistration(tag) }

type ServiceCommand = serviceCommand

type ServiceLifecycle = serviceLifecycle[serviceCommand]

type ServiceManager = serviceManager

func NewServiceManager(host serviceHost) (ServiceManager, error) { return newServiceManager(host) }

type ServiceState = serviceState

func (s serviceLifecycle[R]) Execute(requests <-chan R) error { return s.execute(requests) }

func BackgroundDisposition(dump []byte, label string) (enabled, found bool) {
	return backgroundDisposition(dump, label)
}

func CanonicalExecutable(path string) string { return canonicalExecutable(path) }

func CheckBackground(ctx context.Context, host serviceHost, tag instance.Tag) []service.PreflightCheck {
	return checkBackground(ctx, host, tag)
}

func CheckDropIn(ctx context.Context, host serviceHost, loaded *config.Loaded) service.PreflightCheck {
	return checkDropIn(ctx, host, loaded)
}

func CheckInstancePermissions(dir string, gid int) service.PreflightCheck {
	return checkInstancePermissions(dir, gid)
}

func CheckLeftoverKillSwitch(dir string, probe killSwitchProbe) []service.PreflightCheck {
	return checkLeftoverKillSwitch(dir, probe)
}

func CheckMetricsListen(
	own config.Metrics, others map[instance.Tag]config.Metrics, unreadable []string,
) service.PreflightCheck {
	return checkMetricsListen(own, others, unreadable)
}

func CheckPersistedKillSwitch(ctx context.Context, dir, name string, probe killSwitchProbe) []service.PreflightCheck {
	return checkPersistedKillSwitch(ctx, dir, name, probe)
}

func CheckServiceInstall(ctx context.Context, host serviceHost, loaded *config.Loaded) []service.PreflightCheck {
	return checkServiceInstall(ctx, host, loaded)
}

func CheckToolDirs(host serviceHost, loaded *config.Loaded) service.PreflightCheck {
	return checkToolDirs(host, loaded)
}

func CheckWireGuardConfinement(root string, tag instance.Tag, kernel bool, dropIn string) []service.PreflightCheck {
	return checkWireGuardConfinement(root, tag, kernel, dropIn)
}

func DeleteGroup(ctx context.Context, goos string, exec execFunc) error {
	return deleteGroup(ctx, goos, exec)
}

func DoctorChecks(ctx context.Context, loaded *config.Loaded, host serviceHost) []service.PreflightCheck {
	return doctorChecks(ctx, loaded, host)
}

func EnsureGroup(ctx context.Context, goos string, exec execFunc) error {
	return ensureGroup(ctx, goos, exec)
}

func ExitCode(err error) uint32 { return exitCode(err) }

func InspectConfinement(ctx context.Context, host serviceHost, tag instance.Tag) []service.PreflightCheck {
	return inspectConfinement(ctx, host, tag)
}

func InstallService(
	ctx context.Context, host serviceHost, m serviceManager, tag instance.Tag, home string, out io.Writer,
) error {
	return installService(ctx, host, m, tag, home, out)
}

func InstanceToolDirs(loaded *config.Loaded) []string { return instanceToolDirs(loaded) }

func NewKillSwitchProbeForTest(running, loaded bool, runErr, loadErr error) killSwitchProbe {
	return killSwitchProbe{
		daemonRunning: func(string) (bool, error) { return running, runErr },
		kernelLoaded:  func(context.Context, string) (bool, error) { return loaded, loadErr },
	}
}

func NewRootCmd() *cobra.Command { return newRootCmd() }

func NewRuntime(
	loaded *config.Loaded,
	db *gorm.DB,
	privateKey solana.PrivateKey,
	programClient *program.Client,
	sender *tx.Sender,
	log zerolog.Logger,
	reg prometheus.Registerer,
	pin *rpcpin.Pin,
) (daemon.Runtime, error) {
	return newRuntime(loaded, db, privateKey, programClient, sender, log, reg, pin)
}

func NewSCMRegistrationForTest(args []string, displayName, name string) SCMRegistration {
	return scmRegistration{args: args, displayName: displayName, name: name}
}

func NewServiceHostForTest(
	exec execFunc,
	executable func() (string, error),
	goos string,
	kernelSupport kernelSupportFunc,
	root string,
) serviceHost {
	return serviceHost{
		exec:          exec,
		executable:    executable,
		goos:          goos,
		kernelSupport: kernelSupport,
		root:          root,
	}
}

func NewServiceLifecycleForTest(
	report func(ServiceState),
	run func(context.Context, func()) error,
	startTimeout, stopTimeout time.Duration,
) ServiceLifecycle {
	return serviceLifecycle[serviceCommand]{
		command:      func(cmd serviceCommand) serviceCommand { return cmd },
		report:       report,
		run:          run,
		startTimeout: startTimeout,
		stopTimeout:  stopTimeout,
	}
}

func PrepareRuntime(ctx context.Context, rt daemon.Runtime) error {
	_, err := prepareRuntime(ctx, nil, rt, nil, nil, zerolog.Nop())

	return err
}

// PrepareStart runs the startup checks for loaded with no recovery step. waits reports whether the daemon would
// start serving with the checks deferred to the background.
func PrepareStart(ctx context.Context, loaded *config.Loaded, rpcClient *rpc.Client) (bool, error) {
	startup, err := prepareRuntime(ctx, loaded, nil, rpcClient, nil, zerolog.Nop())

	return startup != nil, err
}

func WaitForChainAccess(ctx context.Context, loaded *config.Loaded, rpcClient *rpc.Client, every time.Duration) error {
	return waitForChainAccess(ctx, loaded, rpcClient, nil, zerolog.Nop(), every, every)
}

func PrepareToolDirs(root string, dirs []string) error { return prepareToolDirs(root, dirs) }

func RenderLaunchdPlist(binary string, tag instance.Tag) string {
	return renderLaunchdPlist(binary, tag)
}

func RenderSystemdService(binary string) string { return renderSystemdService(binary) }

func RenderSystemdSocket() string { return renderSystemdSocket() }

func Serve(ctx context.Context, loaded *config.Loaded, rt daemon.Runtime, ready daemon.Ready) error {
	reg := daemon.NewRegistry()

	return serve(ctx, loaded, rt, reg, reg, zerolog.Nop(), ready, nil)
}

func ServiceHints(goos string, tag instance.Tag) []string { return serviceHints(goos, tag) }

func ServiceHostWGEnforced(h serviceHost, ctx context.Context) (bool, error) {
	return h.wgEnforced(ctx)
}

func UninstallService(ctx context.Context, host serviceHost, m serviceManager, tag instance.Tag) error {
	return uninstallService(ctx, host, m, tag)
}

func UserspaceNames(
	ctx context.Context,
	loaded *config.Loaded,
	kernelSupport kernelSupportFunc,
) ([]service.Name, error) {
	return userspaceNames(ctx, loaded, kernelSupport)
}

func WriteChecks(w io.Writer, format string, checks []service.PreflightCheck) error {
	return writeChecks(w, format, checks)
}

func WriteUserspaceDropIn(root string, tag instance.Tag, names []service.Name) error {
	return writeUserspaceDropIn(root, tag, names)
}

func RunStartChecks(
	ctx context.Context, wait bool, minDelay, maxDelay time.Duration, checks func(context.Context) error,
) error {
	return runStartChecks(ctx, wait, minDelay, maxDelay, checks, zerolog.Nop())
}
