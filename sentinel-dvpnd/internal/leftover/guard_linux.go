//go:build linux

package leftover

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

const guardReleaseWait = 3 * time.Second

var (
	// GuardDied is what the kernel sends the guard when its daemon dies (PR_SET_PDEATHSIG).
	GuardDied os.Signal = syscall.SIGUSR2
	// GuardRelease is what a daemon that stopped cleanly sends: exit without touching anything.
	GuardRelease os.Signal = syscall.SIGUSR1
)

// StartGuardProcess starts argv as the guard child and returns the func that releases it. The kernel signals
// the child with GuardDied when the thread that started it exits, so that thread is kept for the daemon's
// lifetime. The child gets its own process group, so a terminal's Ctrl-C reaches only the daemon.
func StartGuardProcess(argv, env []string) (func() error, error) {
	if len(argv) == 0 {
		return nil, errors.New("starting the tunnel guard: empty command")
	}

	started := make(chan error, 1)
	stop := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		// Never unlocked: when this goroutine returns, its thread ends with it, which is the same
		// parent-death signal a crash sends; release has told the guard to ignore it by then.
		runtime.LockOSThread()

		cmd := exec.Command(argv[0], argv[1:]...) //#nosec G204 -- the daemon's own executable
		cmd.Env = append(os.Environ(), env...)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGUSR2, Setpgid: true}

		if err := cmd.Start(); err != nil {
			started <- fmt.Errorf("starting the tunnel guard: %w", err)

			return
		}

		started <- nil

		<-stop

		if err := cmd.Process.Signal(GuardRelease); err != nil {
			done <- fmt.Errorf("releasing the tunnel guard: %w", err)

			return
		}

		waited := make(chan error, 1)
		go func() { waited <- cmd.Wait() }()

		select {
		case err := <-waited:
			done <- err
		case <-time.After(guardReleaseWait):
			done <- errors.Join(errors.New("the tunnel guard did not exit after release"), cmd.Process.Kill())
		}
	}()

	if err := <-started; err != nil {
		return nil, err
	}

	return func() error {
		close(stop)

		return <-done
	}, nil
}

// RunGuard is the guard child's body: it returns once the daemon (parent) has died and the interfaces are
// removed, or once the daemon released it. A daemon that died before the guard armed is handled at once.
func RunGuard(ctx context.Context, parent int, g Guard) bool {
	// systemd's stop SIGTERMs the whole unit; the daemon's own teardown handles that, not the guard.
	signal.Ignore(syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, GuardDied, GuardRelease)
	defer signal.Stop(sigs)

	if os.Getppid() != parent {
		g.Remove(ctx)

		return true
	}

	return g.Watch(ctx, sigs, GuardDied, GuardRelease)
}
