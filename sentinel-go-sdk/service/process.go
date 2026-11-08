package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const FailedStartStopTimeout = 10 * time.Second

const (
	pollInterval = 250 * time.Millisecond
	startTimeout = 15 * time.Second
	stderrLimit  = 4096
	stopGrace    = 5 * time.Second
)

type Exiter interface {
	Done() <-chan struct{}
	Err() error
}

type Process interface {
	Done() <-chan struct{}
	Err() error
	PID() int
	Stop(ctx context.Context) error
	Wait(ctx context.Context) error
}

type ProcessManager struct {
	StopOnce

	binary     string
	closeOnce  sync.Once
	dir        string
	kill       func(ctx context.Context, path, binary, configFlag, conf string) error
	launchDone <-chan struct{}
	log        zerolog.Logger
	process    Process
	start      func(ctx context.Context, command Command, log zerolog.Logger) (Process, error)
	stopped    chan struct{}
}

func NewProcessManager(binary, dir string, log zerolog.Logger, opts ...ProcessManagerOption) *ProcessManager {
	p := &ProcessManager{
		binary:  binary,
		dir:     dir,
		kill:    KillPIDFile,
		log:     log,
		start:   StartProcess,
		stopped: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *ProcessManager) KillStale(ctx context.Context, configFlag, conf string) error {
	return p.kill(ctx, PIDPath(p.dir, p.binary), p.binary, configFlag, conf)
}

func (p *ProcessManager) Launch(ctx context.Context, command Command, probe func(context.Context) error) error {
	process, err := p.start(ctx, command, p.log)
	if err != nil {
		return fmt.Errorf("starting %s: %w", p.binary, err)
	}

	if err := WritePIDFile(PIDPath(p.dir, p.binary), process.PID()); err != nil {
		return p.abort(ctx, process, "stopping after failed PID write: %w", err)
	}

	if err := PollReadyProcess(ctx, process, probe); err != nil {
		return p.abort(ctx, process, "stopping after failed start: %w", err)
	}

	p.recordExecutable(ctx, process.PID())

	p.launchDone = ctx.Done()
	p.process = process

	return nil
}

// recordExecutable adds the image the ready process runs to its PID file. A wrapper script for the
// binary execs into another file, and without this record a later start would not recognise a copy
// left running by a daemon that was killed, so it would never stop it.
func (p *ProcessManager) recordExecutable(ctx context.Context, pid int) {
	executable, _, err := readProcessArgs(ctx, pid)
	if err != nil {
		p.log.Debug().Err(err).Int("pid", pid).Msg("reading launched executable")

		return
	}

	if err := writePIDRecord(PIDPath(p.dir, p.binary), pid, executable); err != nil {
		p.log.Warn().Err(err).Int("pid", pid).Msg("recording launched executable")
	}
}

func (p *ProcessManager) Started() bool {
	if p.process == nil {
		return false
	}

	select {
	case <-p.stopped:
		return false
	default:
		return true
	}
}

func (p *ProcessManager) Terminate(ctx context.Context) error {
	if p.process == nil {
		return nil
	}

	p.closeOnce.Do(func() { close(p.stopped) })

	if err := p.process.Stop(ctx); err != nil {
		return fmt.Errorf("stopping %s: %w", p.binary, err)
	}

	return RemovePIDFile(PIDPath(p.dir, p.binary))
}

func (p *ProcessManager) Wait(ctx context.Context) error {
	if p.process == nil {
		return fmt.Errorf("waiting for %s: %w", p.binary, ErrServiceNotStarted)
	}

	err := p.process.Wait(ctx)
	if err == nil {
		return nil
	}

	if p.exitRequested() {
		return nil
	}

	return fmt.Errorf("waiting for %s: %w", p.binary, err)
}

func (p *ProcessManager) abort(ctx context.Context, process Process, step string, err error) error {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), FailedStartStopTimeout)
	defer cancel()

	if stopErr := process.Stop(stopCtx); stopErr != nil {
		err = errors.Join(err, fmt.Errorf(step, stopErr))

		return fmt.Errorf("starting %s: %w", p.binary, err)
	}

	if removeErr := RemovePIDFile(PIDPath(p.dir, p.binary)); removeErr != nil {
		err = errors.Join(err, removeErr)
	}

	return fmt.Errorf("starting %s: %w", p.binary, err)
}

func (p *ProcessManager) exitRequested() bool {
	select {
	case <-p.process.Done():
	default:
		return false
	}

	select {
	case <-p.stopped:
		return true
	case <-p.launchDone:
		return true
	default:
		return false
	}
}

type ProcessManagerOption func(*ProcessManager)

var _ Process = (*osProcess)(nil)

type osProcess struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
	pid    int
	tail   tailBuffer
}

func (p *osProcess) Done() <-chan struct{} { return p.done }

func (p *osProcess) Err() error {
	select {
	case <-p.done:
		if p.err == nil {
			return ErrProcessExited
		}

		if tail := p.tail.trimmed(); len(tail) > 0 {
			return fmt.Errorf("%w: %w: %s", ErrProcessExited, p.err, tail)
		}

		return fmt.Errorf("%w: %w", ErrProcessExited, p.err)
	default:
		return nil
	}
}

func (p *osProcess) PID() int { return p.pid }

func (p *osProcess) Stop(ctx context.Context) error {
	p.cancel()

	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("process did not exit: %w", ctx.Err())
	}
}

func (p *osProcess) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		if p.err != nil {
			return fmt.Errorf("%w: %w", ErrProcessExited, p.err)
		}

		return nil
	case <-ctx.Done():
		return fmt.Errorf("process still running: %w", ctx.Err())
	}
}

type tailBuffer struct {
	data []byte
	mu   sync.Mutex
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = append(b.data, p...)
	if len(b.data) > stderrLimit {
		copy(b.data, b.data[len(b.data)-stderrLimit:])
		b.data = b.data[:stderrLimit]
	}

	return len(p), nil
}

func (b *tailBuffer) trimmed() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return bytes.TrimSpace(b.data)
}

func PollReadyProcess(ctx context.Context, p Exiter, probe func(context.Context) error) error {
	return pollUntilReady(ctx, p.Done(), p.Err, probe)
}

func StartProcess(ctx context.Context, command Command, log zerolog.Logger) (Process, error) {
	runCtx, cancel := context.WithCancel(ctx)
	process := &osProcess{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	cmd := newCmd(runCtx, command)
	cmd.Stdout = log.With().Str("process", command.Path).Logger()
	cmd.Stderr = io.MultiWriter(cmd.Stdout, &process.tail)
	cmd.Cancel = func() error { return terminate(cmd) }
	cmd.WaitDelay = stopGrace

	if err := cmd.Start(); err != nil {
		cancel()

		return nil, fmt.Errorf("process not started: %w", err)
	}

	process.pid = cmd.Process.Pid

	wait := func() {
		process.err = cmd.Wait()
		close(process.done)
	}

	go wait()

	return process, nil
}

func WithProcessStarter(
	start func(ctx context.Context, command Command, log zerolog.Logger) (Process, error),
) ProcessManagerOption {
	return func(p *ProcessManager) { p.start = start }
}

func WithStaleKiller(kill func(ctx context.Context, path, binary, configFlag, conf string) error) ProcessManagerOption {
	return func(p *ProcessManager) { p.kill = kill }
}
