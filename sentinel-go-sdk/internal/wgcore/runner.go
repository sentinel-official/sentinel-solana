package wgcore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const userspaceEnv = "WG_QUICK_USERSPACE_IMPLEMENTATION"

//nolint:gochecknoglobals
var serviceInstalled installedFunc

type execFunc func(ctx context.Context, c service.Command) ([]byte, error)

type installedFunc func(name string) (bool, error)

type quickRunner struct {
	wgCLI

	confinable      bool
	goos            string
	runDir          string
	tunnelBinary    string
	tunnelPrefix    []string
	userspaceBinary string
}

// tunnelArgs puts the variant's prefix (a script run by an interpreter, say) before the action.
func (r quickRunner) tunnelArgs(action, conf string) []string {
	return append(slices.Clone(r.tunnelPrefix), action, conf)
}

func (r quickRunner) Cleanup(ctx context.Context, conf string, owned bool) error {
	if !owned {
		return nil
	}

	live, err := liveInterface(interfaceNameFromConf(conf), r.runDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	if err != nil {
		return err
	}

	if _, err := r.Transfer(ctx, live); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("checking %s: %w", live, ctx.Err())
		}

		if tunnelGone(err) {
			return nil
		}

		return err
	}

	return r.Down(ctx, conf)
}

func (r quickRunner) Down(ctx context.Context, conf string) error {
	cmd := service.Command{
		Args:  r.tunnelArgs("down", conf),
		Path:  r.tunnelBinary,
		Unset: []string{userspaceEnv},
	}
	if _, err := r.exec(ctx, cmd); err != nil {
		return fmt.Errorf("%s down %s: %w", r.tunnelBinary, conf, err)
	}

	return nil
}

func (r quickRunner) Up(ctx context.Context, conf string) (string, error) {
	cmd := service.Command{
		Args:  r.tunnelArgs("up", conf),
		Path:  r.tunnelBinary,
		Unset: []string{userspaceEnv},
	}
	if _, err := r.exec(ctx, cmd); err != nil {
		text := err.Error()
		if r.confinable && r.goos == "linux" && r.userspaceBinary != "" &&
			strings.Contains(text, r.userspaceBinary) && strings.Contains(text, "Permission denied") {
			return "", fmt.Errorf("this host confines `%s`; load the WireGuard kernel module: %w", r.binary, err)
		}

		return "", fmt.Errorf("%s up: %w", r.tunnelBinary, err)
	}

	return liveInterface(interfaceNameFromConf(conf), r.runDir)
}

type runner interface {
	Cleanup(ctx context.Context, conf string, owned bool) error
	Down(ctx context.Context, conf string) error
	Transfer(ctx context.Context, iface string) (map[string]service.PeerStats, error)
	RemovePeer(ctx context.Context, iface, publicKey string) error
	SetPeer(ctx context.Context, iface, publicKey string, allowedIPs []netip.Prefix) error
	Up(ctx context.Context, conf string) (string, error)
}

func newRunner(v Variant) runner {
	cli := wgCLI{
		binary: v.Binary,
		exec:   service.Run,
	}
	if runtime.GOOS == "windows" {
		return windowsServiceRunner{
			installed:           serviceInstalled,
			serviceBinary:       v.TunnelBinary,
			tunnelServicePrefix: v.TunnelServicePrefix,
			wgCLI:               cli,
		}
	}

	return quickRunner{
		wgCLI:           cli,
		confinable:      v.Confinable,
		goos:            runtime.GOOS,
		runDir:          v.RunDir,
		tunnelBinary:    v.TunnelBinary,
		tunnelPrefix:    v.TunnelPrefix,
		userspaceBinary: v.UserspaceBinary,
	}
}

type wgCLI struct {
	binary string
	exec   execFunc
}

func (r wgCLI) RemovePeer(ctx context.Context, iface, publicKey string) error {
	cmd := service.Command{
		Args: []string{"set", iface, "peer", publicKey, "remove"},
		Path: r.binary,
	}

	if _, err := r.exec(ctx, cmd); err != nil {
		return fmt.Errorf("%s set remove: %w", r.binary, err)
	}

	return nil
}

func (r wgCLI) SetPeer(ctx context.Context, iface, publicKey string, allowedIPs []netip.Prefix) error {
	allowed := make([]string, 0, len(allowedIPs))
	for _, p := range allowedIPs {
		allowed = append(allowed, p.String())
	}

	cmd := service.Command{
		Args: []string{"set", iface, "peer", publicKey, "allowed-ips", strings.Join(allowed, ",")},
		Path: r.binary,
	}

	if _, err := r.exec(ctx, cmd); err != nil {
		return fmt.Errorf("%s set allowed-ips: %w", r.binary, err)
	}

	return nil
}

func (r wgCLI) Transfer(ctx context.Context, iface string) (map[string]service.PeerStats, error) {
	cmd := service.Command{
		Args: []string{"show", iface, "transfer"},
		Path: r.binary,
	}

	out, err := r.exec(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("%s show transfer: %w", r.binary, err)
	}

	return parseTransfer(string(out))
}

type windowsServiceRunner struct {
	wgCLI

	installed           installedFunc
	serviceBinary       string
	tunnelServicePrefix string
}

func (r windowsServiceRunner) Cleanup(ctx context.Context, conf string, _ bool) error {
	name := r.tunnelServicePrefix + interfaceNameFromConf(conf)

	installed, err := r.installed(name)
	if err != nil {
		return fmt.Errorf("looking up service %s: %w", name, err)
	}

	if !installed {
		return nil
	}

	return r.Down(ctx, conf)
}

func (r windowsServiceRunner) Down(ctx context.Context, conf string) error {
	name := interfaceNameFromConf(conf)

	cmd := service.Command{
		Args: []string{"/uninstalltunnelservice", name},
		Path: r.serviceBinary,
	}
	if _, err := r.exec(ctx, cmd); err != nil {
		return fmt.Errorf("%s /uninstalltunnelservice %s: %w", r.serviceBinary, name, err)
	}

	return nil
}

func (r windowsServiceRunner) Up(ctx context.Context, conf string) (string, error) {
	cmd := service.Command{
		Args: []string{"/installtunnelservice", conf},
		Path: r.serviceBinary,
	}
	if _, err := r.exec(ctx, cmd); err != nil {
		return "", fmt.Errorf("%s /installtunnelservice %s: %w", r.serviceBinary, conf, err)
	}

	return interfaceNameFromConf(conf), nil
}

func interfaceNameFromConf(conf string) string {
	base := conf
	if i := strings.LastIndexAny(conf, `/\\`); i >= 0 {
		base = conf[i+1:]
	}

	return strings.TrimSuffix(base, ".conf")
}

func parseTransfer(out string) (map[string]service.PeerStats, error) {
	transfer := map[string]service.PeerStats{}

	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		fields := strings.Split(sc.Text(), "\t")
		if len(fields) != 3 {
			continue
		}

		rx, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing transfer: %w", err)
		}

		tx, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing transfer: %w", err)
		}

		transfer[fields[0]] = service.PeerStats{
			RxBytes: rx,
			TxBytes: tx,
		}
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scanning transfer: %w", err)
	}

	return transfer, nil
}

func tunnelGone(err error) bool {
	text := err.Error()

	for _, gone := range []string{
		"No such device",
		"No such file or directory",
		"Operation not supported",
		"Protocol not supported",
	} {
		if strings.Contains(text, gone) {
			return true
		}
	}

	return false
}
