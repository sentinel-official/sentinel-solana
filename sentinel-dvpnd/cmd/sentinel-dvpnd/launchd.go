package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const (
	launchDaemonsDir   = "/Library/LaunchDaemons"
	launchdPlistSource = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{{ xml .Label }}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{{ xml .Binary }}</string>
		<string>run</string>
		<string>--instance</string>
		<string>{{ xml .Tag }}</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>StandardOutPath</key>
	<string>{{ xml .LogFile }}</string>
	<key>StandardErrorPath</key>
	<string>{{ xml .LogFile }}</string>
	<key>EnvironmentVariables</key>
	<dict>
		<key>PATH</key>
		<string>/usr/bin:/bin:/usr/sbin:/sbin</string>
	</dict>
</dict>
</plist>
`
)

//nolint:gochecknoglobals
var launchdPlistTemplate = template.Must(
	template.New("launchd-plist").Funcs(template.FuncMap{"xml": escapeXML}).Parse(launchdPlistSource),
)

type launchdManager struct {
	host serviceHost
}

func (m launchdManager) Install(_ context.Context, loaded *config.Loaded) error {
	binary, err := m.host.executable()
	if err != nil {
		return err
	}

	logDir := filepath.Join(m.host.root, filepath.Dir(loaded.Tag.LogPath(m.host.goos)))
	if info, err := os.Lstat(logDir); err == nil && !info.IsDir() {
		return fmt.Errorf("%w: %s", errInvalidDir, logDir)
	}

	if err := os.MkdirAll(logDir, 0o755); err != nil { //#nosec G301 -- launchd logs are world-readable
		return fmt.Errorf("creating log folder: %w", err)
	}

	if err := os.Chmod(logDir, 0o755); err != nil { //#nosec G302 -- launchd logs are world-readable
		return fmt.Errorf("setting log folder mode: %w", err)
	}

	return writeSystemFile(m.plistPath(loaded.Tag), []byte(renderLaunchdPlist(binary, loaded.Tag)))
}

func (m launchdManager) Installed(_ context.Context, tag instance.Tag) (bool, error) {
	if _, err := os.Lstat(m.plistPath(tag)); errors.Is(err, fs.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("checking launch daemon plist: %w", err)
	}

	return true, nil
}

func (launchdManager) Prune(context.Context) error { return nil }

func (m launchdManager) Restart(ctx context.Context, tag instance.Tag) error {
	if !m.loaded(ctx, tag) {
		return m.Start(ctx, tag)
	}

	return m.launchctl(ctx, "kickstart", "-k", "system/"+tag.ServiceName(m.host.goos))
}

func (m launchdManager) Start(ctx context.Context, tag instance.Tag) error {
	if !m.loaded(ctx, tag) {
		if err := m.launchctl(ctx, "bootstrap", "system", m.plistPath(tag)); err != nil {
			return err
		}
	}

	return m.launchctl(ctx, "kickstart", "system/"+tag.ServiceName(m.host.goos))
}

func (m launchdManager) Status(ctx context.Context, tag instance.Tag) (string, error) {
	out, err := m.launchctlOutput(ctx, "print", "system/"+tag.ServiceName(m.host.goos))
	if err != nil {
		return "not loaded", nil
	}

	for line := range strings.SplitSeq(string(out), "\n") {
		if state, ok := strings.CutPrefix(line, "\tstate = "); ok {
			return state, nil
		}
	}

	return "unknown", nil
}

func (m launchdManager) Stop(ctx context.Context, tag instance.Tag) error {
	if !m.loaded(ctx, tag) {
		return nil
	}

	return m.launchctl(ctx, "bootout", "system/"+tag.ServiceName(m.host.goos))
}

func (m launchdManager) Uninstall(ctx context.Context, tag instance.Tag) error {
	if err := m.Stop(ctx, tag); err != nil {
		return err
	}

	if err := os.Remove(m.plistPath(tag)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("removing launch daemon plist: %w", err)
	}

	return nil
}

func (m launchdManager) launchctl(ctx context.Context, args ...string) error {
	_, err := m.launchctlOutput(ctx, args...)

	return err
}

func (m launchdManager) launchctlOutput(ctx context.Context, args ...string) ([]byte, error) {
	command := service.Command{
		Args: args,
		Path: "launchctl",
	}

	out, err := m.host.exec(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", args[0], err)
	}

	return out, nil
}

func (m launchdManager) loaded(ctx context.Context, tag instance.Tag) bool {
	return m.launchctl(ctx, "print", "system/"+tag.ServiceName(m.host.goos)) == nil
}

func (m launchdManager) plistPath(tag instance.Tag) string {
	return filepath.Join(m.host.root, launchDaemonsDir, tag.ServiceName(m.host.goos)+".plist")
}

type launchdTemplateData struct {
	Binary  string
	Label   string
	LogFile string
	Tag     string
}

func backgroundDisposition(dump []byte, label string) (enabled, found bool) {
	for _, item := range regexp.MustCompile(`(?m)^\s*#\d+:`).Split(string(dump), -1) {
		var disposition string

		var ours bool

		for line := range strings.SplitSeq(item, "\n") {
			key, value, _ := strings.Cut(strings.TrimSpace(line), ":")

			switch value = strings.TrimSpace(value); key {
			case "Disposition":
				disposition = value
			case "Identifier", "URL":
				ours = ours || strings.HasSuffix(strings.TrimSuffix(value, ".plist"), label)
			}
		}

		if ours {
			return !strings.Contains(disposition, "disabled"), true
		}
	}

	return false, false
}

func checkBackground(ctx context.Context, host serviceHost, tag instance.Tag) []service.PreflightCheck {
	label := tag.ServiceName("darwin")
	if _, err := os.Lstat(filepath.Join(host.root, launchDaemonsDir, label+".plist")); err != nil {
		return nil
	}

	dump, err := host.exec(ctx, service.Command{
		Args: []string{"dumpbtm"},
		Path: "sfltool",
	})
	enabled, found := backgroundDisposition(dump, label)
	severity, detail := service.SeverityOK, "macOS allows "+label+" to run in the background"

	switch {
	case errors.Is(err, exec.ErrNotFound):
		return nil
	case err != nil:
		severity, detail = service.SeverityWarning, "cannot read the background items ("+err.Error()+"); run doctor with sudo"
	case !found:
		severity = service.SeverityWarning
		detail = label + " is not in the background items yet; it appears after the first launchctl bootstrap"
	case !enabled:
		severity = service.SeverityError
		detail = "macOS switched off " + label +
			": System Settings > General > Login Items & Extensions > Allow in the Background"
	}

	return []service.PreflightCheck{service.NewPreflightCheck(backgroundCheckName, severity, detail)}
}

func escapeXML(s string) string {
	var b strings.Builder

	_ = xml.EscapeText(&b, []byte(s))

	return b.String()
}

func renderLaunchdPlist(binary string, tag instance.Tag) string {
	data := launchdTemplateData{
		Binary:  binary,
		Label:   tag.ServiceName("darwin"),
		LogFile: tag.LogPath("darwin"),
		Tag:     string(tag),
	}

	var b strings.Builder

	if err := launchdPlistTemplate.Execute(&b, data); err != nil {
		panic(fmt.Errorf("executing launchd plist template: %w", err))
	}

	return b.String()
}
