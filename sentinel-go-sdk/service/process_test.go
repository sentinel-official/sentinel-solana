package service_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const fakePID = 4242

var (
	errCrashed    = errors.New("crashed")
	errSignalled  = errors.New("signalled")
	errStopFailed = errors.New("stop failed")
)

type fakeProcess struct {
	done    chan struct{}
	exitErr error
	once    sync.Once
	pid     int
	stopErr error
}

func newFakeProcess() *fakeProcess {
	return &fakeProcess{done: make(chan struct{}), pid: fakePID}
}

func (p *fakeProcess) Done() <-chan struct{} { return p.done }

func (p *fakeProcess) Err() error {
	select {
	case <-p.done:
		return service.ErrProcessExited
	default:
		return nil
	}
}

func (p *fakeProcess) PID() int { return p.pid }

func (p *fakeProcess) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("stopping fake: %w", err)
	}

	if p.stopErr != nil {
		return p.stopErr
	}

	p.exit(errSignalled)

	return nil
}

func (p *fakeProcess) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		if p.exitErr != nil {
			return fmt.Errorf("%w: %w", service.ErrProcessExited, p.exitErr)
		}

		return nil
	case <-ctx.Done():
		return fmt.Errorf("process still running: %w", ctx.Err())
	}
}

func (p *fakeProcess) exit(err error) {
	p.once.Do(func() {
		p.exitErr = err
		close(p.done)
	})
}

func TestPollReadyProcess_ExitCancelsBlockingProbe(t *testing.T) {
	t.Parallel()

	process, err := service.StartProcess(t.Context(), sleepCommand(), zerolog.Nop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	entered := make(chan struct{})

	result := make(chan error, 1)
	go func() {
		result <- service.PollReadyProcess(ctx, process, func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()

			return ctx.Err()
		})
	}()

	<-entered

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()

	require.NoError(t, process.Stop(stopCtx))

	select {
	case err := <-result:
		require.ErrorIs(t, err, service.ErrProcessExited)
	case <-time.After(time.Second):
		cancel()
		t.Fatalf("readiness probe did not return after process exit: %v", <-result)
	}
}

func TestPollReadyProcess_ExitWinsCanceledContext(t *testing.T) {
	t.Parallel()

	for range 32 {
		p := &fakeExiter{done: make(chan struct{})}
		ctx, cancel := context.WithCancel(t.Context())
		probe := func(context.Context) error {
			close(p.done)
			cancel()

			return errors.New("not ready")
		}
		err := service.PollReadyProcess(ctx, p, probe)
		require.ErrorIs(t, err, service.ErrProcessExited)
		require.Contains(t, err.Error(), "boom")
	}
}

func TestPollReadyProcess_Ready(t *testing.T) {
	t.Parallel()

	process, err := service.StartProcess(t.Context(), sleepCommand(), zerolog.Nop())
	require.NoError(t, err)

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		require.NoError(t, process.Stop(stopCtx))
	}()

	require.NoError(t, process.Err())

	calls := 0
	probe := func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}

		return nil
	}
	require.NoError(t, service.PollReadyProcess(t.Context(), process, probe))
	require.Equal(t, 3, calls)
}

func TestPollReadyProcess_ReturnsOnExit(t *testing.T) {
	t.Parallel()

	command := service.Command{Args: []string{"-c", "echo boom >&2; exit 3"}, Path: "sh"}
	if runtime.GOOS == goosWindows {
		command = service.Command{Args: []string{"/c", "echo boom 1>&2 & exit 3"}, Path: "cmd"}
	}

	process, err := service.StartProcess(t.Context(), command, zerolog.Nop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	probe := func(context.Context) error { return errors.New("not ready") }
	err = service.PollReadyProcess(ctx, process, probe)
	require.ErrorIs(t, err, service.ErrProcessExited)
	require.Contains(t, err.Error(), "boom")

	<-process.Done()
	err = service.PollReadyProcess(ctx, process, func(context.Context) error { return nil })
	require.ErrorIs(t, err, service.ErrProcessExited)
}

func TestProcess_Err_AfterSuccessfulExit(t *testing.T) {
	t.Parallel()

	command := service.Command{Args: []string{"-c", "exit 0"}, Path: "sh"}
	if runtime.GOOS == goosWindows {
		command = service.Command{Args: []string{"/c", "exit 0"}, Path: "cmd"}
	}

	process, err := service.StartProcess(t.Context(), command, zerolog.Nop())
	require.NoError(t, err)
	<-process.Done()
	require.ErrorIs(t, process.Err(), service.ErrProcessExited)
}

func TestProcess_Err_StderrTailBounded(t *testing.T) {
	t.Parallel()

	process, err := service.StartProcess(t.Context(), service.Command{
		Args: []string{"-test.run=^TestServiceChildProcess$"},
		Env:  []string{"SERVICE_TEST_CHILD=1"},
		Path: os.Args[0],
	}, zerolog.Nop())
	require.NoError(t, err)
	<-process.Done()
	require.EqualError(t, process.Err(), "process exited: exit status 3: "+strings.Repeat("x", 4092)+"boom")
}

func TestProcess_Stop(t *testing.T) {
	t.Parallel()

	process, err := service.StartProcess(t.Context(), sleepCommand(), zerolog.Nop())
	require.NoError(t, err)

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, process.Stop(stopCtx))
}

func TestProcess_Wait(t *testing.T) {
	t.Parallel()

	process, err := service.StartProcess(t.Context(), exitCommand(), zerolog.Nop())
	require.NoError(t, err)

	err = process.Wait(t.Context())
	require.ErrorIs(t, err, service.ErrProcessExited)
	require.EqualError(t, err, "process exited: exit status 3")
	require.EqualError(t, process.Err(), "process exited: exit status 3")
}

func TestProcessManager_Launch_ReadinessFailureKeepsPIDFileOnStopFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	child := newFakeProcess()
	child.stopErr = errStopFailed
	pm := service.NewProcessManager("bin", dir, zerolog.Nop(),
		service.WithProcessStarter(newFakeStarter(child)),
	)

	err := pm.Launch(t.Context(), service.Command{}, func(context.Context) error {
		return errors.New("not ready")
	})
	require.ErrorIs(t, err, errStopFailed)

	_, statErr := os.Stat(service.PIDPath(dir, "bin"))
	require.NoError(t, statErr)
}

func TestProcessManager_Launch_ReadinessFailureStopsChild(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	dir := t.TempDir()
	child := newFakeProcess()
	pm := service.NewProcessManager("bin", dir, zerolog.Nop(),
		service.WithProcessStarter(func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
			return child, nil
		}),
	)

	err := pm.Launch(ctx, service.Command{}, func(context.Context) error {
		return errors.New("not ready")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, pm.Started())

	select {
	case <-child.done:
	default:
		require.Fail(t, "child was not stopped")
	}

	_, statErr := os.Stat(service.PIDPath(dir, "bin"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestProcessManager_Launch_WritesPIDFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	child := newFakeProcess()
	pm := service.NewProcessManager("bin", dir, zerolog.Nop(),
		service.WithProcessStarter(newFakeStarter(child)),
	)

	require.NoError(t, pm.Launch(t.Context(), service.Command{}, func(context.Context) error { return nil }))
	require.True(t, pm.Started())

	data, err := os.ReadFile(service.PIDPath(dir, "bin"))
	require.NoError(t, err)

	first, _, _ := strings.Cut(string(data), "\n")
	require.Equal(t, strconv.Itoa(child.pid), first)
}

func TestProcessManager_Launch_RecordsExecutable(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "linux" {
		t.Skip("reads /proc")
	}

	dir := t.TempDir()
	pm := service.NewProcessManager("sh", dir, zerolog.Nop())
	require.NoError(t, pm.Launch(t.Context(), service.Command{Path: "sh", Args: []string{"-c", "sleep 30"}},
		func(context.Context) error { return nil }))

	t.Cleanup(func() { _ = pm.Terminate(context.WithoutCancel(t.Context())) })

	data, err := os.ReadFile(service.PIDPath(dir, "sh"))
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2)

	exe, err := os.Readlink("/proc/" + lines[0] + "/exe")
	require.NoError(t, err)
	require.Equal(t, exe, lines[1])
}

func TestProcessManager_Terminate_KeepsPIDFileOnFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	child := newFakeProcess()
	child.stopErr = errStopFailed
	pm := startFake(t, t.Context(), dir, child)

	err := pm.Terminate(t.Context())
	require.ErrorIs(t, err, errStopFailed)
	require.ErrorContains(t, err, "stopping bin:")

	_, statErr := os.Stat(service.PIDPath(dir, "bin"))
	require.NoError(t, statErr)
}

func TestProcessManager_Terminate_NotStarted(t *testing.T) {
	t.Parallel()

	pm := service.NewProcessManager("bin", t.TempDir(), zerolog.Nop())

	require.NoError(t, pm.Terminate(t.Context()))
}

func TestProcessManager_Terminate_RemovesPIDFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pm := startFake(t, t.Context(), dir, newFakeProcess())

	require.NoError(t, pm.Terminate(t.Context()))
	require.False(t, pm.Started())

	_, statErr := os.Stat(service.PIDPath(dir, "bin"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestProcessManager_Wait_NilAfterLaunchContextCanceled(t *testing.T) {
	t.Parallel()

	launchCtx, cancel := context.WithCancel(t.Context())
	pm := startFake(t, launchCtx, t.TempDir(), newFakeProcess())
	result := waitAsync(t.Context(), pm)

	cancel()

	require.NoError(t, <-result)
}

func TestProcessManager_Wait_NilAfterTerminate(t *testing.T) {
	t.Parallel()

	pm := startFake(t, t.Context(), t.TempDir(), newFakeProcess())
	result := waitAsync(t.Context(), pm)

	require.NoError(t, pm.Terminate(t.Context()))
	require.NoError(t, <-result)
}

func TestProcessManager_Wait_ProcessExited(t *testing.T) {
	t.Parallel()

	child := newFakeProcess()
	pm := startFake(t, t.Context(), t.TempDir(), child)

	child.exit(errCrashed)

	require.ErrorIs(t, pm.Wait(t.Context()), service.ErrProcessExited)
}

func TestServiceChildProcess(t *testing.T) {
	t.Parallel()

	if os.Getenv("SERVICE_TEST_CHILD") != "1" {
		return
	}

	_, _ = os.Stderr.WriteString(strings.Repeat("x", 4100) + "boom")

	os.Exit(3)
}

func exitCommand() service.Command {
	if runtime.GOOS == goosWindows {
		return service.Command{Args: []string{"/c", "exit 3"}, Path: "cmd"}
	}

	return service.Command{Args: []string{"-c", "exit 3"}, Path: "sh"}
}

func newFakeStarter(
	p *fakeProcess,
) func(ctx context.Context, cmd service.Command, log zerolog.Logger) (service.Process, error) {
	return func(ctx context.Context, _ service.Command, _ zerolog.Logger) (service.Process, error) {
		go func() {
			<-ctx.Done()
			p.exit(errSignalled)
		}()

		return p, nil
	}
}

func sleepCommand() service.Command {
	if runtime.GOOS == goosWindows {
		return service.Command{Args: []string{"-Command", "Start-Sleep", "-Seconds", "30"}, Path: "powershell"}
	}

	return service.Command{Args: []string{"30"}, Path: "sleep"}
}

func startFake(t *testing.T, ctx context.Context, dir string, child *fakeProcess) *service.ProcessManager {
	t.Helper()

	pm := service.NewProcessManager("bin", dir, zerolog.Nop(),
		service.WithProcessStarter(newFakeStarter(child)),
	)
	require.NoError(t, pm.Launch(ctx, service.Command{}, func(context.Context) error { return nil }))

	return pm
}

func waitAsync(ctx context.Context, pm *service.ProcessManager) <-chan error {
	result := make(chan error, 1)

	go func() { result <- pm.Wait(ctx) }()

	return result
}
