package amneziawg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// The awg-quick that amneziawg-tools installs on macOS (src/wg-quick/darwin.bash) is wg-quick's script with
// amneziawg-go swapped in: it still calls `wg` and still reads /var/run/wireguard, while amneziawg-go writes
// the interface name and socket under /var/run/amneziawg. get_real_interface then never finds the interface,
// so `awg-quick up` fails. The client runs a copy with both fixed, under a bash 4 (the script needs it;
// macOS ships 3.2).

var (
	errQuickUnrecognised = errors.New("awg-quick is not the darwin script this fix knows")
	errUnsafeToolPath    = errors.New("tool path has characters the script can't take unquoted")

	// wgCall matches `wg` used as a command: at a line start or after a shell separator, followed by a space.
	wgCall       = regexp.MustCompile("(?m)(^|[\\s(;|&`])wg ")
	safeToolPath = regexp.MustCompile(`^/[A-Za-z0-9/._+-]+$`)
)

const (
	quickRunDirOld    = "/var/run/wireguard"
	quickUserspaceOld = ":-amneziawg-go}"
	bashTimeout       = 5 * time.Second
)

// patchQuickDarwin rewrites the darwin awg-quick to call awg at awgPath, to use /var/run/amneziawg, and to
// start amneziawg-go from userspacePath. It refuses a script it doesn't recognise.
func patchQuickDarwin(src []byte, awgPath, userspacePath string) ([]byte, error) {
	for _, p := range []string{awgPath, userspacePath} {
		if !safeToolPath.MatchString(p) {
			return nil, fmt.Errorf("%w: %q", errUnsafeToolPath, p)
		}
	}

	text := string(src)
	if !strings.Contains(text, quickUserspaceOld) || !strings.Contains(text, runDir) {
		return nil, errQuickUnrecognised
	}

	text = strings.ReplaceAll(text, quickRunDirOld, runDir)
	text = strings.ReplaceAll(text, quickUserspaceOld, ":-"+userspacePath+"}")

	return []byte(wgCall.ReplaceAllString(text, "${1}"+awgPath+" ")), nil
}

type darwinToolset struct {
	bashVersion func(path string) (int, error)
	bashes      []string
	lookPath    func(name string) (string, error)
	readFile    func(path string) ([]byte, error)
}

func darwinTools() darwinToolset {
	return darwinToolset{
		bashVersion: bashMajor,
		bashes:      []string{"/opt/homebrew/bin/bash", "/usr/local/bin/bash", "/bin/bash"},
		lookPath:    service.LookPath,
		readFile:    os.ReadFile,
	}
}

func bashMajor(path string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), bashTimeout)
	defer cancel()

	out, err := exec.CommandContext(ctx, path, "-c", "echo ${BASH_VERSINFO[0]}").Output() //#nosec G204 -- fixed paths
	if err != nil {
		return 0, fmt.Errorf("running %s: %w", path, err)
	}

	n, err := strconv.Atoi(string(bytes.TrimSpace(out)))
	if err != nil {
		return 0, fmt.Errorf("reading %s version: %w", path, err)
	}

	return n, nil
}

func (t darwinToolset) bash4() (string, error) {
	for _, path := range t.bashes {
		if n, err := t.bashVersion(path); err == nil && n >= 4 {
			return path, nil
		}
	}

	return "", fmt.Errorf("%w: bash 4 or later (looked in %s)", service.ErrBinaryNotFound, strings.Join(t.bashes, ", "))
}

type darwinQuickPaths struct {
	awg, bash, quick, userspace string
}

func (t darwinToolset) find() (darwinQuickPaths, error) {
	var (
		paths darwinQuickPaths
		err   error
	)

	if paths.bash, err = t.bash4(); err != nil {
		return paths, err
	}

	for name, dst := range map[string]*string{tunnelBinary(): &paths.quick, binary: &paths.awg, userspaceBinary: &paths.userspace} {
		if *dst, err = t.lookPath(name); err != nil {
			return paths, err
		}
	}

	return paths, nil
}

// prepareDarwinQuick writes the fixed awg-quick next to the session's other files and returns the bash to
// run it with and the script's path.
func prepareDarwinQuick(t darwinToolset, p service.Placement) (string, string, error) {
	paths, err := t.find()
	if err != nil {
		return "", "", err
	}

	src, err := t.readFile(paths.quick)
	if err != nil {
		return "", "", fmt.Errorf("reading %s: %w", paths.quick, err)
	}

	fixed, err := patchQuickDarwin(src, paths.awg, paths.userspace)
	if err != nil {
		return "", "", fmt.Errorf("%s: %w", paths.quick, err)
	}

	script := p.ToolPath(".awg-quick.bash")
	if err := service.WriteOwned(script, fixed, "#"); err != nil {
		return "", "", err
	}

	return paths.bash, script, nil
}

// darwinQuickChecks warns when the fixed awg-quick can't be prepared; with everything in place it has nothing to say.
func darwinQuickChecks(t darwinToolset) []service.PreflightCheck {
	paths, err := t.find()
	if err == nil {
		var src []byte

		if src, err = t.readFile(paths.quick); err == nil {
			_, err = patchQuickDarwin(src, paths.awg, paths.userspace)
		}
	}

	if err == nil {
		return nil
	}

	return []service.PreflightCheck{service.NewPreflightCheck("service_platform", service.SeverityWarning,
		darwinQuickNeeds+err.Error())}
}

const darwinQuickNeeds = "AmneziaWG on macOS needs awg-quick, awg and amneziawg-go from amneziawg-tools and a bash 4 or later: "
