package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

const (
	pidCheckTimeout = time.Second
	pidPollInterval = 100 * time.Millisecond
	pidStartSlack   = 2 * time.Second
)

//nolint:gochecknoglobals
var (
	killOwnedImage = func(context.Context, int, func(string, time.Time) bool) error {
		return errors.ErrUnsupported
	}
	readProcessArgs = func(context.Context, int) (string, []string, error) {
		return "", nil, errors.ErrUnsupported
	}
)

type processIdentity struct {
	binary     string
	conf       string
	configFlag string
	// launched is the image the process was seen running after it started, read from the PID file.
	// A wrapper script execs into another binary, so binary alone would not recognise it.
	launched string
}

func KillPIDFile(ctx context.Context, path, binary, configFlag, conf string) error {
	identity := processIdentity{
		binary:     binary,
		conf:       conf,
		configFlag: configFlag,
	}

	return killPIDFile(ctx, path, identity, runtime.GOOS)
}

func PIDPath(dir, binary string) string {
	return filepath.Join(dir, binary+".pid")
}

func RemovePIDFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing PID file %s: %w", path, err)
	}

	return nil
}

func WritePIDFile(path string, pid int) error {
	return writePIDRecord(path, pid, "")
}

// writePIDRecord writes the PID and, when known, the executable the process runs on a second line.
func writePIDRecord(path string, pid int, executable string) error {
	if pid <= 1 {
		return fmt.Errorf("%w: PID %d", validate.ErrInvalidConfig, pid)
	}

	if strings.ContainsAny(executable, "\r\n") || (executable != "" && !filepath.IsAbs(executable)) {
		return fmt.Errorf("%w: executable %q", validate.ErrInvalidConfig, executable)
	}

	content := strconv.Itoa(pid) + "\n"
	if executable != "" {
		content += executable + "\n"
	}

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("writing PID file %s: %w", path, err)
	}

	return nil
}

func parsePIDRecord(raw []byte) (int, string, error) {
	first, rest, _ := strings.Cut(string(raw), "\n")

	pid, err := strconv.Atoi(strings.TrimSpace(first))
	if err != nil {
		return 0, "", fmt.Errorf("parsing PID: %w", err)
	}

	executable := strings.TrimSpace(rest)
	if executable != "" && !filepath.IsAbs(executable) {
		executable = ""
	}

	return pid, executable, nil
}

func killImagePID(ctx context.Context, path string, pid int, binary string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("reading PID file %s: %w", path, err)
	}

	err = killOwnedImage(ctx, pid, func(image string, started time.Time) bool {
		return ownsImage(image, binary, started, info.ModTime())
	})
	if err != nil {
		return fmt.Errorf("killing PID %d: %w", pid, err)
	}

	return nil
}

func killPIDFile(ctx context.Context, path string, identity processIdentity, goos string) (result error) {
	raw, err := os.ReadFile(path) //#nosec G304
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("reading PID file %s: %w", path, err)
	}

	remove := true
	defer func() {
		if !remove {
			return
		}

		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("removing PID file %s: %w", path, err))
		}
	}()

	pid, launched, err := parsePIDRecord(raw)
	if err != nil || pid <= 1 || pid == os.Getpid() {
		return nil
	}

	identity.launched = launched

	if identity.binary == "" || identity.configFlag == "" || identity.conf == "" {
		remove = false

		return fmt.Errorf("%w: process identity is incomplete", validate.ErrInvalidConfig)
	}

	if err := ctx.Err(); err != nil {
		remove = false

		return fmt.Errorf("checking PID file context: %w", err)
	}

	if goos == "windows" {
		err := killImagePID(ctx, path, pid, identity.binary)
		remove = err == nil

		return err
	}

	owned, err := ownsPID(ctx, pid, identity)
	if err != nil {
		remove = false

		return fmt.Errorf("checking PID file %s: %w", path, err)
	}

	if !owned {
		return nil
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		remove = false

		return fmt.Errorf("finding PID %d: %w", pid, err)
	}

	if err := process.Signal(syscall.SIGTERM); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return nil
		}

		remove = false

		return fmt.Errorf("terminating PID %d: %w", pid, err)
	}

	retain, err := waitPIDExit(ctx, pid, identity, process)
	remove = !retain

	return err
}

func matchesCommand(args []string, identity processIdentity) bool {
	if len(args) < 3 {
		return false
	}

	name := filepath.Base(args[0])
	if name != filepath.Base(identity.binary) && (identity.launched == "" || name != filepath.Base(identity.launched)) {
		return false
	}

	for i := 1; i+1 < len(args); i++ {
		if args[i] == identity.configFlag && args[i+1] == identity.conf {
			return true
		}
	}

	return false
}

func ownsImage(image, binary string, started, written time.Time) bool {
	if started.After(written.Add(pidStartSlack)) {
		return false
	}

	path, err := resolveForMatch(binary)

	return err == nil && strings.EqualFold(filepath.Clean(image), filepath.Clean(path))
}

func ownsPID(ctx context.Context, pid int, identity processIdentity) (bool, error) {
	executable, args, err := readProcessArgs(ctx, pid)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return false, nil
	}

	if errors.Is(err, syscall.EINVAL) && pidGone(pid) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("reading PID %d command: %w", pid, err)
	}

	if !matchesCommand(args, identity) {
		return false, nil
	}

	if identity.launched != "" && executable == identity.launched {
		return true, nil
	}

	path, err := resolveForMatch(identity.binary)
	if err != nil {
		return false, fmt.Errorf("resolving managed executable: %w", err)
	}

	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return false, fmt.Errorf("resolving managed executable: %w", err)
	}

	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return false, fmt.Errorf("resolving PID %d executable: %w", pid, err)
	}

	return executable == path, nil
}

func pidGone(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	err = process.Signal(syscall.Signal(0))

	return errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH)
}

func waitPIDExit(ctx context.Context, pid int, identity processIdentity, process *os.Process) (bool, error) {
	grace := time.NewTimer(stopGrace)
	defer grace.Stop()

	graceC := grace.C

	tick := time.NewTicker(pidPollInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pidCheckTimeout)
			owned, err := ownsPID(checkCtx, pid, identity)

			cancel()

			if err != nil || owned {
				return true, fmt.Errorf("waiting for PID %d: %w", pid, ctx.Err())
			}

			return false, fmt.Errorf("waiting for PID %d: %w", pid, ctx.Err())
		case <-graceC:
			owned, err := ownsPID(ctx, pid, identity)
			if err != nil {
				return true, err
			}

			if !owned {
				return false, nil
			}

			if err := process.Kill(); err != nil {
				if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
					return false, nil
				}

				return true, fmt.Errorf("killing PID %d: %w", pid, err)
			}

			graceC = nil
		case <-tick.C:
		}

		owned, err := ownsPID(ctx, pid, identity)
		if err != nil {
			return true, err
		}

		if !owned {
			return false, nil
		}
	}
}
