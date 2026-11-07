package main_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"

	main "github.com/sentinel-official/sentinel-dvpnd/cmd/sentinel-dvpnd"
)

type fakeControlledService struct {
	controls []svc.Cmd
	state    svc.State
}

func (s *fakeControlledService) Control(cmd svc.Cmd) (svc.Status, error) {
	s.controls = append(s.controls, cmd)
	s.state = svc.Stopped

	return svc.Status{State: s.state}, nil
}

func (s *fakeControlledService) Query() (svc.Status, error) { return svc.Status{State: s.state}, nil }

type fakeInstalledService struct {
	actions     []mgr.RecoveryAction
	config      mgr.Config
	configErr   error
	deleted     bool
	deleteErr   error
	nonCrash    bool
	nonCrashErr error
	recoveryErr error
	reset       uint32
}

func (s *fakeInstalledService) Delete() error {
	s.deleted = true

	return s.deleteErr
}

func (s *fakeInstalledService) SetRecoveryActions(actions []mgr.RecoveryAction, reset uint32) error {
	s.actions = actions
	s.reset = reset

	return s.recoveryErr
}

func (s *fakeInstalledService) SetRecoveryActionsOnNonCrashFailures(enabled bool) error {
	s.nonCrash = enabled

	return s.nonCrashErr
}

func (s *fakeInstalledService) UpdateConfig(cfg mgr.Config) error {
	s.config = cfg

	return s.configErr
}

func TestConfigureService(t *testing.T) {
	t.Parallel()

	cfg := main.ServiceConfig(main.NewSCMRegistration("c0"), `C:\sentinel-dvpnd.exe`)
	fake := &fakeInstalledService{}
	require.NoError(t, main.ConfigureService(fake, cfg))
	require.Equal(t, cfg, fake.config)
	require.True(t, fake.nonCrash)
	require.False(t, fake.deleted)
	require.Len(t, fake.actions, 3)
	require.EqualValues(t, 86400, fake.reset)

	// Backs off; Windows repeats the last (5 min) action for every later failure.
	for i, want := range []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute} {
		require.Equal(t, mgr.ServiceRestart, fake.actions[i].Type)
		require.Equal(t, want, fake.actions[i].Delay)
	}
}

func TestConfigureService_Failure(t *testing.T) {
	t.Parallel()

	for _, step := range []string{"config", "recovery", "non-crash recovery"} {
		t.Run(step, func(t *testing.T) {
			t.Parallel()

			failure := errors.New("configuration failed")
			fake := &fakeInstalledService{}

			switch step {
			case "config":
				fake.configErr = failure
			case "recovery":
				fake.recoveryErr = failure
			case "non-crash recovery":
				fake.nonCrashErr = failure
			}

			require.ErrorIs(t, main.ConfigureService(fake, mgr.Config{}), failure)
			require.False(t, fake.deleted)

			if step == "config" {
				require.Empty(t, fake.actions)
			}
		})
	}
}

func TestExecuteWindowsService(t *testing.T) {
	t.Parallel()

	for _, command := range []svc.Cmd{svc.Stop, svc.Shutdown} {
		t.Run(map[svc.Cmd]string{svc.Stop: "stop", svc.Shutdown: "shutdown"}[command], func(t *testing.T) {
			t.Parallel()

			requests := make(chan svc.ChangeRequest)
			statuses := make(chan svc.Status)
			done := make(chan uint32, 1)

			go func() {
				_, code := main.ExecuteWindowsService(requests, statuses, func(ctx context.Context, ready func()) error {
					ready()
					<-ctx.Done()

					return ctx.Err()
				})
				done <- code
			}()

			require.Equal(t, svc.StartPending, serviceStatus(t, statuses).State)
			running := serviceStatus(t, statuses)
			require.Equal(t, svc.Running, running.State)
			require.Equal(t, svc.AcceptStop|svc.AcceptShutdown, running.Accepts)

			requests <- svc.ChangeRequest{Cmd: svc.Interrogate}

			require.Equal(t, running, serviceStatus(t, statuses))

			requests <- svc.ChangeRequest{Cmd: command}

			require.Equal(t, svc.StopPending, serviceStatus(t, statuses).State)

			select {
			case code := <-done:
				require.Zero(t, code)
			case <-time.After(time.Second):
				t.Fatal("service did not stop")
			}
		})
	}
}

func TestRollbackService(t *testing.T) {
	t.Parallel()

	cause := errors.New("configuration failed")

	fake := &fakeInstalledService{}
	require.Equal(t, cause, main.RollbackService(fake, cause))
	require.True(t, fake.deleted)

	deleteErr := errors.New("access denied")
	fake = &fakeInstalledService{deleteErr: deleteErr}

	err := main.RollbackService(fake, cause)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, deleteErr)
	require.ErrorContains(t, err, "rolling back service registration")
}

func TestServiceConfig(t *testing.T) {
	t.Parallel()

	cfg := main.ServiceConfig(main.NewSCMRegistration("c0"), `C:\Program Files\sentinel-dvpn\sentinel-dvpnd.exe`)
	require.Equal(t, `"C:\Program Files\sentinel-dvpn\sentinel-dvpnd.exe" run --instance c0`, cfg.BinaryPathName)
	require.Equal(t, "Sentinel dVPN daemon (c0)", cfg.DisplayName)
	require.Equal(t, cfg.DisplayName, cfg.Description)
	require.Equal(t, []string{"BFE", "Nsi"}, cfg.Dependencies)
	require.EqualValues(t, mgr.StartAutomatic, cfg.StartType)
	require.Equal(t, "LocalSystem", cfg.ServiceStartName)
	require.EqualValues(t, windows.SERVICE_SID_TYPE_UNRESTRICTED, cfg.SidType)
}

func TestServiceCreateConfig(t *testing.T) {
	t.Parallel()

	cfg := main.ServiceCreateConfig(main.NewSCMRegistration("c0"))
	require.Equal(t, "Sentinel dVPN daemon (c0)", cfg.DisplayName)
	require.Empty(t, cfg.Description)
	require.EqualValues(t, windows.SERVICE_SID_TYPE_NONE, cfg.SidType)
	require.False(t, cfg.DelayedAutoStart)
	require.EqualValues(t, windows.SERVICE_WIN32_OWN_PROCESS, cfg.ServiceType)
	require.Equal(t, "LocalSystem", cfg.ServiceStartName)
}

func TestStopService(t *testing.T) {
	t.Parallel()

	fake := &fakeControlledService{state: svc.Running}
	require.NoError(t, main.StopService(t.Context(), fake))
	require.Equal(t, []svc.Cmd{svc.Stop}, fake.controls)
}

func TestStopService_Canceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	fake := &fakeControlledService{state: svc.Running}
	require.ErrorIs(t, main.StopService(ctx, fake), context.Canceled)
	require.Empty(t, fake.controls)
}

func serviceStatus(t *testing.T, statuses <-chan svc.Status) svc.Status {
	t.Helper()

	select {
	case status := <-statuses:
		return status
	case <-time.After(time.Second):
		t.Fatal("service status timed out")
	}

	return svc.Status{}
}
