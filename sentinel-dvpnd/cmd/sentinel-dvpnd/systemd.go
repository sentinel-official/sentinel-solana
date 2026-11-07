package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"text/template"
	"unicode"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/amneziawg"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/wireguard"
)

const (
	systemdInstance instance.Tag = "%i"

	systemdServiceSource = `[Unit]
Description=Sentinel dVPN daemon (%i)
Requires={{ .Name }}.socket
After=network-online.target {{ .Name }}.socket
Wants=network-online.target

[Service]
Type=notify
ExecStart={{ .Binary }} run --instance %i
Restart=always
RestartSec=5
TimeoutStopSec=180
KillMode=mixed
AmbientCapabilities=CAP_NET_ADMIN CAP_NET_BIND_SERVICE
# NoNewPrivileges stays off: wg-quick switches wg, ip, nft and sysctl into their AppArmor profiles,
# which the kernel refuses under no_new_privs. systemd turns NoNewPrivileges on by itself when a unit
# without CAP_SYS_ADMIN uses seccomp options (SystemCallFilter=, RestrictAddressFamilies=, ...), so
# adding User= together with such options brings that breakage back.
ProtectSystem=strict
PrivateTmp=yes
ReadWritePaths={{ .StateDir }}
ReadWritePaths=-/etc/resolv.conf
{{- range .ToolDirs }}
ReadWritePaths=-{{ . }}
{{- end }}
EnvironmentFile=-{{ .EnvFile }}

[Install]
WantedBy=multi-user.target
`
	systemdServiceUnit  = instance.DaemonName + "@.service"
	systemdSocketSource = `[Unit]
Description=Sentinel dVPN daemon control socket (%i)

[Socket]
ListenStream={{ .Endpoint }}
Symlinks={{ .StateDir }}/daemon.sock
SocketMode=0660
SocketGroup={{ .Group }}
RemoveOnStop=yes

[Install]
WantedBy=sockets.target
`
	systemdSocketUnit = instance.DaemonName + "@.socket"
	systemdUnitDir    = "/etc/systemd/system"
	systemdWantsDir   = systemdUnitDir + "/multi-user.target.wants"
)

var errInvalidBinaryPath = errors.New("executable path contains characters that systemd would split or expand")

//nolint:gochecknoglobals
var (
	systemdServiceTemplate = template.Must(template.New("systemd-service").Parse(systemdServiceSource))
	systemdSocketTemplate  = template.Must(template.New("systemd-socket").Parse(systemdSocketSource))
)

type systemdManager struct {
	host serviceHost
}

func (m systemdManager) Install(ctx context.Context, loaded *config.Loaded) error {
	binary, err := m.host.executable()
	if err != nil {
		return err
	}

	if !safeSystemdPath(binary) {
		return fmt.Errorf("%w: %q", errInvalidBinaryPath, binary)
	}

	if err := writeSystemFile(m.unitPath(systemdServiceUnit), []byte(renderSystemdService(binary))); err != nil {
		return err
	}

	if err := writeSystemFile(m.unitPath(systemdSocketUnit), []byte(renderSystemdSocket())); err != nil {
		return err
	}

	if err := prepareToolDirs(m.host.root, instanceToolDirs(loaded)); err != nil {
		return err
	}

	names, err := userspaceNames(ctx, loaded, m.host.kernelSupport)
	if err != nil {
		return err
	}

	if err := writeUserspaceDropIn(m.host.root, loaded.Tag, names); err != nil {
		return err
	}

	if err := m.systemctl(ctx, "daemon-reload"); err != nil {
		return err
	}

	name := loaded.Tag.ServiceName(m.host.goos)

	return m.systemctl(ctx, "enable", name+".socket", name+".service")
}

func (m systemdManager) Installed(_ context.Context, tag instance.Tag) (bool, error) {
	link := filepath.Join(m.host.root, systemdWantsDir, tag.ServiceName(m.host.goos)+".service")

	if _, err := os.Lstat(link); errors.Is(err, fs.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("checking enabled service: %w", err)
	}

	return true, nil
}

func (m systemdManager) Prune(ctx context.Context) error {
	for _, name := range []string{systemdServiceUnit, systemdSocketUnit} {
		if err := os.Remove(m.unitPath(name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("removing unit template: %w", err)
		}
	}

	return m.systemctl(ctx, "daemon-reload")
}

func (m systemdManager) Restart(ctx context.Context, tag instance.Tag) error {
	return m.systemctl(ctx, "restart", tag.ServiceName(m.host.goos)+".service")
}

func (m systemdManager) Start(ctx context.Context, tag instance.Tag) error {
	return m.systemctl(ctx, "start", tag.ServiceName(m.host.goos)+".service")
}

func (m systemdManager) Status(ctx context.Context, tag instance.Tag) (string, error) {
	name := tag.ServiceName(m.host.goos) + ".service"

	out, err := m.systemctlOutput(ctx, "show", "--property=ActiveState", "--value", name)

	return strings.TrimSpace(string(out)), err
}

func (m systemdManager) Stop(ctx context.Context, tag instance.Tag) error {
	name := tag.ServiceName(m.host.goos)

	return m.systemctl(ctx, "stop", name+".socket", name+".service")
}

func (m systemdManager) Uninstall(ctx context.Context, tag instance.Tag) error {
	if _, err := os.Stat(m.unitPath(systemdServiceUnit)); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("checking service template: %w", err)
	}

	name := tag.ServiceName(m.host.goos)

	if err := m.systemctl(ctx, "disable", "--now", name+".service", name+".socket"); err != nil {
		return err
	}

	if err := writeUserspaceDropIn(m.host.root, tag, nil); err != nil {
		return err
	}

	return m.systemctl(ctx, "daemon-reload")
}

func (m systemdManager) systemctl(ctx context.Context, args ...string) error {
	_, err := m.systemctlOutput(ctx, args...)

	return err
}

func (m systemdManager) systemctlOutput(ctx context.Context, args ...string) ([]byte, error) {
	command := service.Command{
		Args: args,
		Path: "systemctl",
	}

	out, err := m.host.exec(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", args[0], err)
	}

	return out, nil
}

func (m systemdManager) unitPath(name string) string {
	return filepath.Join(m.host.root, systemdUnitDir, name)
}

type systemdTemplateData struct {
	Binary   string
	Endpoint string
	EnvFile  string
	Group    string
	Name     string
	StateDir string
	ToolDirs []string
}

func executeSystemdTemplate(tmpl *template.Template, binary string) string {
	data := systemdTemplateData{
		Binary:   binary,
		Endpoint: systemdInstance.Endpoint("linux"),
		EnvFile:  systemdInstance.EnvPath(),
		Group:    instance.GroupName,
		Name:     systemdInstance.ServiceName("linux"),
		StateDir: systemdInstance.StateDir("linux"),
		ToolDirs: service.ToolDirs(),
	}

	var b strings.Builder

	if err := tmpl.Execute(&b, data); err != nil {
		panic(fmt.Errorf("executing systemd template: %w", err))
	}

	return b.String()
}

func hasKernelVariant(name service.Name) bool {
	return name == amneziawg.Name || name == wireguard.Name
}

func instanceToolDirs(loaded *config.Loaded) []string {
	dirs := make(map[string]struct{}, len(loaded.Services))

	for name := range loaded.Services {
		if dir := loaded.Placement(name).ToolDir; dir != "" {
			dirs[dir] = struct{}{}
		}
	}

	return slices.Sorted(maps.Keys(dirs))
}

func prepareToolDirParent(root, dir string) error {
	path := filepath.Join(root, dir)

	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("checking tool folder parent: %w", err)
	}

	if err := prepareToolDirParent(root, filepath.Dir(dir)); err != nil {
		return err
	}

	if err := os.Mkdir(path, 0o755); err != nil { //#nosec G301 -- official tool folders are world-readable
		return fmt.Errorf("creating tool folder parent: %w", err)
	}

	if err := os.Chmod(path, 0o755); err != nil { //#nosec G302 -- official tool folders are world-readable
		return fmt.Errorf("setting tool folder parent mode: %w", err)
	}

	return nil
}

func prepareToolDirs(root string, dirs []string) error {
	for _, dir := range dirs {
		if err := prepareToolDirParent(root, filepath.Dir(dir)); err != nil {
			return err
		}

		if err := os.Mkdir(filepath.Join(root, dir), 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("creating tool folder: %w", err)
		}
	}

	return nil
}

func renderSystemdService(binary string) string {
	return executeSystemdTemplate(systemdServiceTemplate, binary)
}

func renderSystemdSocket() string {
	return executeSystemdTemplate(systemdSocketTemplate, "")
}

func renderUserspaceDropIn(names []service.Name) string {
	var b strings.Builder

	b.WriteString("[Service]\n")

	for _, name := range slices.Sorted(slices.Values(names)) {
		b.WriteString("RuntimeDirectory=" + name.String() + "\n")
	}

	b.WriteString("RuntimeDirectoryPreserve=yes\n")

	return b.String()
}

func safeSystemdPath(path string) bool {
	return strings.IndexFunc(path, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r) || strings.ContainsRune(`"'\%$`, r)
	}) < 0
}

func userspaceDropInPath(tag instance.Tag) string {
	return filepath.Join(systemdUnitDir, tag.ServiceName("linux")+".service.d", "userspace.conf")
}

func userspaceNames(
	ctx context.Context,
	loaded *config.Loaded,
	kernelSupport kernelSupportFunc,
) ([]service.Name, error) {
	var names []service.Name

	for _, name := range slices.Sorted(maps.Keys(loaded.Services)) {
		if !hasKernelVariant(name) {
			continue
		}

		supported, err := kernelSupport(ctx, name.String())
		if err != nil {
			return nil, err
		}

		if !supported {
			names = append(names, name)
		}
	}

	return names, nil
}

func writeUserspaceDropIn(root string, tag instance.Tag, names []service.Name) error {
	path := filepath.Join(root, userspaceDropInPath(tag))
	dir := filepath.Dir(path)

	if info, err := os.Lstat(dir); err == nil && !info.IsDir() {
		return fmt.Errorf("%w: %s", errInvalidDir, dir)
	}

	if len(names) == 0 {
		for _, target := range []string{path, dir} {
			err := os.Remove(target)
			if err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, syscall.ENOTEMPTY) {
				return fmt.Errorf("removing userspace drop-in: %w", err)
			}
		}

		return nil
	}

	if err := os.Mkdir(dir, 0o755); err == nil { //#nosec G301 -- systemd drop-in folders are world-readable
		if err := os.Chmod(dir, 0o755); err != nil { //#nosec G302 -- systemd drop-in folders are world-readable
			return fmt.Errorf("setting userspace drop-in folder mode: %w", err)
		}
	} else if !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("creating userspace drop-in folder: %w", err)
	}

	return writeSystemFile(path, []byte(renderUserspaceDropIn(names)))
}
