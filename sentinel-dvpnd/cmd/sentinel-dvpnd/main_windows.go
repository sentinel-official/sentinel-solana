package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
	"golang.org/x/sys/windows/svc/mgr"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/daemon"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
)

const (
	servicePollInterval = 250 * time.Millisecond
	serviceResetPeriod  = 24 * time.Hour
)

// serviceRestartDelays: after a crash the service restarts quickly, then backs off. Windows
// repeats the last action for every later failure, so a daemon that keeps failing (preflight
// cannot reach the RPC) retries every 5 min. A flat 5 s restarted it ~6 times a minute for days
// and kept the RPC provider answering 429, which kept preflight failing.
//
//nolint:gochecknoglobals // fixed policy
var serviceRestartDelays = []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute}

const (
	startWaitHint = 120 * time.Second
	stopWaitHint  = 60 * time.Second
)

//nolint:gochecknoglobals
var scmStateNames = map[svc.State]string{
	svc.ContinuePending: "continue pending",
	svc.Paused:          "paused",
	svc.PausePending:    "pause pending",
	svc.Running:         "running",
	svc.StartPending:    "start pending",
	svc.Stopped:         "stopped",
	svc.StopPending:     "stop pending",
}

type controlledService interface {
	Control(cmd svc.Cmd) (svc.Status, error)
	Query() (svc.Status, error)
}

type installedService interface {
	Delete() error
	SetRecoveryActions(actions []mgr.RecoveryAction, reset uint32) error
	SetRecoveryActionsOnNonCrashFailures(enabled bool) error
	UpdateConfig(cfg mgr.Config) error
}

type scmManager struct {
	host serviceHost
}

func (m scmManager) Install(_ context.Context, loaded *config.Loaded) error {
	binary, err := m.host.executable()
	if err != nil {
		return err
	}

	return registerService(loaded.Tag, binary)
}

func (scmManager) Installed(_ context.Context, tag instance.Tag) (bool, error) {
	m, err := mgr.Connect()
	if err != nil {
		return false, fmt.Errorf("connecting to the service manager (run as administrator): %w", err)
	}
	defer func() { _ = m.Disconnect() }()

	s, err := m.OpenService(newSCMRegistration(tag).name)
	if errors.Is(err, windows.ERROR_SERVICE_DOES_NOT_EXIST) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("opening service: %w", err)
	}

	_ = s.Close()

	return true, nil
}

func (scmManager) Prune(context.Context) error { return nil }

func (m scmManager) Restart(ctx context.Context, tag instance.Tag) error {
	if err := m.Stop(ctx, tag); err != nil {
		return err
	}

	return m.Start(ctx, tag)
}

func (scmManager) Start(_ context.Context, tag instance.Tag) error {
	return useService(tag, func(s *mgr.Service) error {
		if err := s.Start(); err != nil && !errors.Is(err, windows.ERROR_SERVICE_ALREADY_RUNNING) {
			return fmt.Errorf("starting service: %w", err)
		}

		return nil
	})
}

func (scmManager) Status(_ context.Context, tag instance.Tag) (string, error) {
	var state string

	err := useService(tag, func(s *mgr.Service) error {
		status, err := s.Query()
		if err != nil {
			return fmt.Errorf("querying service: %w", err)
		}

		state = cmp.Or(scmStateNames[status.State], "unknown")

		return nil
	})

	return state, err
}

func (scmManager) Stop(ctx context.Context, tag instance.Tag) error {
	return useService(tag, func(s *mgr.Service) error {
		ctx, cancel := context.WithTimeout(ctx, stopWaitHint)
		defer cancel()

		return stopService(ctx, s)
	})
}

func (scmManager) Uninstall(ctx context.Context, tag instance.Tag) error {
	return unregisterService(ctx, tag)
}

type serviceHandler struct {
	events *eventlog.Log
	flags  *pflag.FlagSet
	opts   *instanceOptions
	out    io.Writer
	run    func(context.Context, func()) error
}

func (h *serviceHandler) Execute(
	_ []string,
	requests <-chan svc.ChangeRequest,
	status chan<- svc.Status,
) (bool, uint32) {
	run := h.run
	if run == nil {
		run = func(ctx context.Context, ready func()) error { return runInstance(ctx, h.opts, h.flags, h.out, ready) }
	}

	lifecycle := serviceLifecycle[svc.ChangeRequest]{
		command:      toServiceCommand,
		report:       func(state serviceState) { status <- toServiceStatus(state) },
		run:          run,
		startTimeout: startWaitHint,
		stopTimeout:  stopWaitHint,
	}

	err := lifecycle.execute(requests)
	if err != nil && exitCode(err) != 0 {
		_, _ = fmt.Fprintf(h.out, "service stopped: %v\n", err)
		if h.events != nil {
			if eventErr := h.events.Error(1, err.Error()); eventErr != nil {
				_, _ = fmt.Fprintf(h.out, "writing event log: %v\n", eventErr)
			}
		}
	}

	code := exitCode(err)

	return code != 0, code
}

//nolint:gochecknoinits
func init() {
	scmServiceManager = func(host serviceHost) serviceManager { return scmManager{host: host} }
}

func configureService(s installedService, cfg mgr.Config) error {
	if err := s.UpdateConfig(cfg); err != nil {
		return fmt.Errorf("configuring service: %w", err)
	}

	actions := make([]mgr.RecoveryAction, len(serviceRestartDelays))
	for i := range actions {
		actions[i] = mgr.RecoveryAction{
			Delay: serviceRestartDelays[i],
			Type:  mgr.ServiceRestart,
		}
	}

	if err := s.SetRecoveryActions(actions, uint32(serviceResetPeriod.Seconds())); err != nil {
		return fmt.Errorf("setting recovery actions: %w", err)
	}

	if err := s.SetRecoveryActionsOnNonCrashFailures(true); err != nil {
		return fmt.Errorf("enabling recovery for service failures: %w", err)
	}

	return nil
}

func fileGroupID(info fs.FileInfo) (int, bool) { return 0, false }

func installEventLogSource(source string) error {
	path := `SYSTEM\CurrentControlSet\Services\EventLog\Application\` + source

	key, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err == nil {
		_ = key.Close()

		return nil
	}

	if !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		return fmt.Errorf("checking event log source: %w", err)
	}

	if err := eventlog.InstallAsEventCreate(source, eventlog.Error|eventlog.Warning|eventlog.Info); err != nil {
		cleanupErr := eventlog.Remove(source)
		if errors.Is(cleanupErr, windows.ERROR_FILE_NOT_FOUND) {
			cleanupErr = nil
		}

		return errors.Join(fmt.Errorf("registering event log source: %w", err), cleanupErr)
	}

	return nil
}

func prepareInstanceDir(dir, group string) error {
	if err := os.MkdirAll(dir, instanceDirMode); err != nil {
		return fmt.Errorf("creating instance folder: %w", err)
	}

	sddl := "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)"

	if group == "" {
		user, err := windows.GetCurrentProcessToken().GetTokenUser()
		if err != nil {
			return fmt.Errorf("resolving instance folder owner: %w", err)
		}

		sddl += "(A;OICI;FA;;;" + user.User.Sid.String() + ")"
	}

	descriptor, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		return fmt.Errorf("building instance folder permissions: %w", err)
	}

	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("reading instance folder permissions: %w", err)
	}

	if err := windows.SetNamedSecurityInfo(dir, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
		return fmt.Errorf("securing instance folder (run as administrator): %w", err)
	}

	return nil
}

func registerService(tag instance.Tag, binary string) error {
	registration := newSCMRegistration(tag)

	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("connecting to the service manager (run as administrator): %w", err)
	}

	defer func() { _ = m.Disconnect() }()

	s, err := m.OpenService(registration.name)

	created := errors.Is(err, windows.ERROR_SERVICE_DOES_NOT_EXIST)
	if created {
		s, err = m.CreateService(registration.name, binary, serviceCreateConfig(registration), registration.args...)
	}

	if err != nil {
		return fmt.Errorf("registering the service: %w", err)
	}

	defer func() { _ = s.Close() }()

	err = configureService(s, serviceConfig(registration, binary))
	if err == nil {
		err = installEventLogSource(registration.name)
	}

	if err != nil && created {
		return rollbackService(s, err)
	}

	return err
}

func rollbackService(s installedService, cause error) error {
	if err := s.Delete(); err != nil {
		return errors.Join(cause, fmt.Errorf("rolling back service registration: %w", err))
	}

	return cause
}

func runDaemon(ctx context.Context, opts *instanceOptions, flags *pflag.FlagSet) error {
	isService, err := svc.IsWindowsService()
	if err != nil {
		return fmt.Errorf("detecting the service context: %w", err)
	}

	if !isService {
		return runInstance(ctx, opts, flags, os.Stdout, daemon.NotifyReady())
	}

	return runService(opts, flags)
}

func runService(opts *instanceOptions, flags *pflag.FlagSet) error {
	tag, err := opts.parse()
	if err != nil {
		return err
	}

	registration := newSCMRegistration(tag)

	logFile, err := os.OpenFile(tag.LogPath("windows"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening service log: %w", err)
	}
	defer func() { _ = logFile.Close() }()

	events, err := eventlog.Open(registration.name)
	if err != nil {
		return fmt.Errorf("opening service event log: %w", err)
	}
	defer func() { _ = events.Close() }()

	handler := &serviceHandler{
		events: events,
		flags:  flags,
		opts:   opts,
		out:    logFile,
	}
	if err := svc.Run(registration.name, handler); err != nil {
		return fmt.Errorf("running as a service: %w", err)
	}

	return nil
}

func serviceConfig(registration scmRegistration, binary string) mgr.Config {
	return mgr.Config{
		BinaryPathName:   windows.ComposeCommandLine(append([]string{binary}, registration.args...)),
		Dependencies:     []string{"BFE", "Nsi"},
		Description:      registration.displayName,
		DisplayName:      registration.displayName,
		ServiceStartName: "LocalSystem",
		ServiceType:      windows.SERVICE_WIN32_OWN_PROCESS,
		SidType:          windows.SERVICE_SID_TYPE_UNRESTRICTED,
		StartType:        mgr.StartAutomatic,
	}
}

func serviceCreateConfig(registration scmRegistration) mgr.Config {
	cfg := serviceConfig(registration, "")
	cfg.Description = ""
	cfg.SidType = windows.SERVICE_SID_TYPE_NONE
	cfg.DelayedAutoStart = false

	return cfg
}

func stopService(ctx context.Context, s controlledService) error {
	ticker := time.NewTicker(servicePollInterval)
	defer ticker.Stop()

	requested := false

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("waiting for service to stop: %w", err)
		}

		status, err := s.Query()
		if err != nil {
			return fmt.Errorf("querying service: %w", err)
		}

		if status.State == svc.Stopped {
			return nil
		}

		if !requested && status.State != svc.StartPending && status.State != svc.StopPending {
			if _, err := s.Control(svc.Stop); err != nil && !errors.Is(err, windows.ERROR_SERVICE_NOT_ACTIVE) {
				return fmt.Errorf("stopping service: %w", err)
			}

			requested = true
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for service to stop: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func toServiceCommand(req svc.ChangeRequest) serviceCommand {
	if req.Cmd == svc.Interrogate {
		return serviceCommandInterrogate
	}

	if req.Cmd == svc.Stop || req.Cmd == svc.Shutdown {
		return serviceCommandStop
	}

	return serviceCommandIgnore
}

func toServiceStatus(state serviceState) svc.Status {
	switch state {
	case serviceStateStarting:
		return svc.Status{
			State:    svc.StartPending,
			WaitHint: uint32(startWaitHint.Milliseconds()),
		}
	case serviceStateRunning:
		return svc.Status{
			State:   svc.Running,
			Accepts: svc.AcceptStop | svc.AcceptShutdown,
		}
	case serviceStateStopping:
		return svc.Status{
			State:    svc.StopPending,
			WaitHint: uint32(stopWaitHint.Milliseconds()),
		}
	}

	return svc.Status{}
}

func unregisterService(ctx context.Context, tag instance.Tag) error {
	registration := newSCMRegistration(tag)

	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("connecting to the service manager (run as administrator): %w", err)
	}
	defer func() { _ = m.Disconnect() }()

	s, err := m.OpenService(registration.name)
	if errors.Is(err, windows.ERROR_SERVICE_DOES_NOT_EXIST) {
		if err := eventlog.Remove(registration.name); err != nil && !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
			return fmt.Errorf("removing event log source: %w", err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("opening service: %w", err)
	}

	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithTimeout(ctx, stopWaitHint)
	defer cancel()

	if err := stopService(ctx, s); err != nil {
		return err
	}

	removed := true

	if err := eventlog.Remove(registration.name); err != nil {
		if !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
			return fmt.Errorf("removing event log source: %w", err)
		}

		removed = false
	}

	if err := s.Delete(); err != nil {
		failure := fmt.Errorf("deleting the service: %w", err)

		if removed {
			if restoreErr := installEventLogSource(registration.name); restoreErr != nil {
				return errors.Join(failure, fmt.Errorf("restoring event log source: %w", restoreErr))
			}
		}

		return failure
	}

	return nil
}

func useService(tag instance.Tag, use func(s *mgr.Service) error) error {
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("connecting to the service manager (run as administrator): %w", err)
	}
	defer func() { _ = m.Disconnect() }()

	s, err := m.OpenService(newSCMRegistration(tag).name)
	if err != nil {
		return fmt.Errorf("opening service: %w", err)
	}
	defer func() { _ = s.Close() }()

	return use(s)
}
