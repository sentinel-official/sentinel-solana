package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

var (
	ErrBinaryNotFound  = errors.New("binary not found")
	ErrProcessExited   = errors.New("process exited")
	ErrProcessNotReady = errors.New("process not ready")
)

type Command struct {
	Args  []string
	Dir   string
	Env   []string
	Path  string
	Stdin io.Reader
	Unset []string
}

// LookPath resolves a tool to the absolute path of an executable that only trusted accounts can
// replace (see trustedExecutable). A bare name is searched in PATH, the platform's usual install
// folders and the folders protocol drivers register. A copy that fails the check is skipped, and
// when only such copies exist the error names the first one. An explicit path must be absolute
// and pass the check itself.
func LookPath(name string) (string, error) {
	candidates := []string{name}

	if filepath.Base(name) == name {
		candidates = candidates[:0]
		for _, dir := range searchDirs(runtime.GOOS) {
			candidates = append(candidates, filepath.Join(dir, name))
		}
	}

	var untrusted error

	for _, candidate := range candidates {
		path, err := exec.LookPath(candidate)
		if err != nil {
			continue
		}

		if err := trustedExecutable(path); err != nil {
			if untrusted == nil {
				untrusted = err
			}

			continue
		}

		return path, nil
	}

	if untrusted != nil {
		return "", untrusted
	}

	return "", fmt.Errorf("%w: %s", ErrBinaryNotFound, name)
}

// resolveForMatch resolves a tool the way the daemon runs it, to recognise processes it started.
// It never runs anything, so an explicit path that does not pass LookPath (missing now, or
// refused) is kept as written and can still match a process an earlier run left behind.
func resolveForMatch(name string) (string, error) {
	path, err := LookPath(name)
	if err == nil || filepath.Base(name) == name {
		return path, err
	}

	return name, nil
}

// CommandContext is exec.CommandContext for a tool the daemon starts: name resolves through
// LookPath, and the child gets a PATH of trusted folders only. When name does not resolve, the
// returned command fails to start with the lookup error.
func CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	path, err := LookPath(name)
	if err != nil {
		// Keep the name for error messages; Start returns cmd.Err before anything runs.
		path = name
	}

	cmd := exec.CommandContext(ctx, path, args...)
	if err != nil {
		cmd.Err = err
	}

	cmd.Env = toolEnv(nil, nil)

	return cmd
}

func PollReady(ctx context.Context, probe func(context.Context) error) error {
	return pollUntilReady(ctx, nil, nil, probe)
}

func Run(ctx context.Context, command Command) ([]byte, error) {
	var stdout, stderr bytes.Buffer

	cmd := newCmd(ctx, command)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("running %s: %w: %s", command.Path, err, bytes.TrimSpace(stderr.Bytes()))
	}

	return stdout.Bytes(), nil
}

func environWithout(env, names []string, goos string) []string {
	equal := func(a, b string) bool { return a == b }
	if goos == "windows" {
		equal = strings.EqualFold
	}

	return slices.DeleteFunc(slices.Clone(env), func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")

		return slices.ContainsFunc(names, func(name string) bool { return equal(key, name) })
	})
}

func exited(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func knownBinaryDirs(goos string) []string {
	switch goos {
	case "darwin":
		return []string{
			"/opt/homebrew/bin",
			"/opt/homebrew/sbin",
			"/usr/local/bin",
			"/usr/local/sbin",
			"/usr/sbin",
			"/sbin",
		}
	default:
		return nil
	}
}

func newCmd(ctx context.Context, command Command) *exec.Cmd {
	cmd := CommandContext(ctx, command.Path, command.Args...)
	cmd.Dir = command.Dir
	cmd.Env = toolEnv(command.Unset, command.Env)
	cmd.Stdin = command.Stdin
	configure(cmd)

	return cmd
}

// toolEnv is this process's environment without the unset names, with PATH replaced by the
// trusted folders, then the extra entries (which may set PATH again on purpose).
func toolEnv(unset, extra []string) []string {
	env := environWithout(os.Environ(), append(slices.Clone(unset), "PATH"), runtime.GOOS)

	return append(append(env, "PATH="+trustedPathEnv(runtime.GOOS)), extra...)
}

func pollUntilReady(
	ctx context.Context,
	done <-chan struct{},
	exitErr func() error,
	probe func(context.Context) error,
) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(startTimeout)
	}

	probeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	go func() {
		select {
		case <-done:
			cancel()
		case <-probeCtx.Done():
		}
	}()

	var lastErr error

	for probeCtx.Err() == nil {
		if exited(done) {
			return exitErr()
		}

		lastErr = probe(probeCtx)
		if lastErr == nil {
			if exited(done) {
				return exitErr()
			}

			return nil
		}

		select {
		case <-done:
			return exitErr()
		case <-time.After(pollInterval):
		case <-probeCtx.Done():
			if exited(done) {
				return exitErr()
			}

			if ctx.Err() != nil {
				return fmt.Errorf("waiting until ready: %w: %w", ctx.Err(), lastErr)
			}

			if ok {
				return fmt.Errorf("waiting until ready: %w: %w", probeCtx.Err(), lastErr)
			}

			return fmt.Errorf("%w: %w", ErrProcessNotReady, lastErr)
		}
	}

	if exited(done) {
		return exitErr()
	}

	if ok || ctx.Err() != nil {
		err := ctx.Err()
		if err == nil {
			err = context.DeadlineExceeded
		}

		if lastErr == nil {
			return fmt.Errorf("waiting until ready: %w", err)
		}

		return fmt.Errorf("waiting until ready: %w: %w", err, lastErr)
	}

	return fmt.Errorf("%w: %w", ErrProcessNotReady, lastErr)
}
