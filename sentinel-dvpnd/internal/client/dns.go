package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const (
	resolvconfBackupFileName = "resolv.conf.bak"
	resolvconfPath           = "/etc/resolv.conf"
)

type dnsBackend interface {
	Apply(ctx context.Context, iface string, servers []string) error
	Cleanup(ctx context.Context) error
	Revert(ctx context.Context, iface string) error
}

func newDNSBackend(kind, dir string) dnsBackend {
	switch runtime.GOOS {
	case "darwin":
		return newDarwinDNSBackend(kind, dir, runCommand)
	case "windows":
		return newWindowsDNSBackend(kind, dir, runCommand)
	default:
		return newLinuxDNSBackend(kind, resolvconfPath, filepath.Join(dir, resolvconfBackupFileName))
	}
}

type linuxDNSBackend struct {
	dnsBackend

	resolvconf resolvconfBackend
}

func newLinuxDNSBackend(kind, path, backupPath string) dnsBackend {
	resolvconf := resolvconfBackend{
		backupPath: backupPath,
		path:       path,
	}

	switch kind {
	case config.DNSResolvconf:
		return resolvconf
	case config.DNSResolvectl:
		return linuxDNSBackend{
			dnsBackend: resolvectlBackend{},
			resolvconf: resolvconf,
		}
	default:
		return linuxDNSBackend{
			dnsBackend: noopBackend{},
			resolvconf: resolvconf,
		}
	}
}

func (b linuxDNSBackend) Cleanup(ctx context.Context) error {
	if err := b.resolvconf.Cleanup(ctx); err != nil {
		return err
	}

	return b.dnsBackend.Cleanup(ctx)
}

type noopBackend struct{}

func (noopBackend) Apply(context.Context, string, []string) error {
	return nil
}

func (noopBackend) Cleanup(context.Context) error { return nil }

func (noopBackend) Revert(context.Context, string) error {
	return nil
}

type resolvconfBackend struct {
	backupPath string
	path       string
}

func (b resolvconfBackend) Apply(_ context.Context, _ string, servers []string) error {
	if _, err := os.Stat(b.backupPath); err != nil {
		current, err := os.ReadFile(b.path)
		if err != nil {
			return fmt.Errorf("reading resolv.conf: %w", err)
		}

		if err := os.WriteFile(b.backupPath, current, 0o644); err != nil { //#nosec G306 G703
			return fmt.Errorf("backing up resolv.conf: %w", err)
		}
	}

	var content strings.Builder
	for _, s := range servers {
		content.WriteString("nameserver ")
		content.WriteString(s)
		content.WriteString("\n")
	}

	if err := os.WriteFile(b.path, []byte(content.String()), 0o644); err != nil { //#nosec G306 G703
		return fmt.Errorf("writing resolv.conf: %w", err)
	}

	return nil
}

func (b resolvconfBackend) Cleanup(ctx context.Context) error {
	return b.Revert(ctx, "")
}

func (b resolvconfBackend) Revert(_ context.Context, _ string) error {
	saved, err := os.ReadFile(b.backupPath) //#nosec G304

	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("reading resolv.conf backup: %w", err)
	}

	if err := os.WriteFile(b.path, saved, 0o644); err != nil { //#nosec G306 G703
		return fmt.Errorf("restoring resolv.conf: %w", err)
	}

	if err := os.Remove(b.backupPath); err != nil {
		return fmt.Errorf("removing resolv.conf backup: %w", err)
	}

	return nil
}

type resolvectlBackend struct{}

func (resolvectlBackend) Apply(ctx context.Context, iface string, servers []string) error {
	args := append([]string{"dns", iface}, servers...)
	if output, err := service.CommandContext(ctx, "resolvectl", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("resolvectl dns: %w: %s", err, output)
	}

	if output, err := service.CommandContext(ctx, "resolvectl", "domain", iface, "~.").CombinedOutput(); err != nil {
		return fmt.Errorf("resolvectl domain: %w: %s", err, output)
	}

	return nil
}

func (resolvectlBackend) Cleanup(context.Context) error { return nil }

func (resolvectlBackend) Revert(ctx context.Context, iface string) error {
	present, err := interfaceExists(iface)
	if err != nil {
		return fmt.Errorf("resolvectl revert: %w", err)
	}

	if !present {
		return nil
	}

	if output, err := service.CommandContext(ctx, "resolvectl", "revert", iface).CombinedOutput(); err != nil {
		return fmt.Errorf("resolvectl revert: %w: %s", err, output)
	}

	return nil
}

func (r *Runtime) applyDNS(ctx context.Context, iface string, placeholder *connection) error {
	if !r.owns(placeholder) {
		return ErrStoppedDuringConnect
	}

	actx, cancel := r.withTimeout(ctx)
	defer cancel()

	if err := r.dnsBackend.Apply(actx, iface, r.cfg.Network.DNSServers); err != nil {
		rctx, cancelRevert := r.withDetachedTimeout(ctx)
		defer cancelRevert()

		if revertErr := r.dnsBackend.Revert(rctx, iface); revertErr != nil {
			return errors.Join(fmt.Errorf("applying dns: %w", err), fmt.Errorf("reverting partial dns: %w", revertErr))
		}

		return fmt.Errorf("applying dns: %w", err)
	}

	r.mu.Lock()
	r.dnsIface = iface
	r.mu.Unlock()

	if err := r.markDNS(actx, true); err != nil {
		_ = r.revertDNS(ctx)

		return fmt.Errorf("recording dns: %w", err)
	}

	return nil
}

func (r *Runtime) markDNS(ctx context.Context, applied bool) error {
	return r.db.WithContext(ctx).Model(&Firewall{}).Where("1 = 1").Update("dns_applied", applied).Error
}

func (r *Runtime) revertDNS(ctx context.Context) error {
	r.mu.Lock()
	iface := r.dnsIface
	r.mu.Unlock()

	if iface == "" {
		return nil
	}

	ctx, cancel := r.withDetachedTimeout(ctx)
	defer cancel()

	if err := r.dnsBackend.Revert(ctx, iface); err != nil {
		r.log.Error().Err(err).Str("iface", iface).Msg("reverting dns; the next stop retries it")

		return fmt.Errorf("reverting dns: %w", err)
	}

	r.mu.Lock()
	if r.dnsIface == iface {
		r.dnsIface = ""
	}
	r.mu.Unlock()

	if err := r.markDNS(ctx, false); err != nil {
		r.log.Warn().Err(err).Str("iface", iface).Msg("recording the dns revert; the next run reverts again")
	}

	return nil
}

func interfaceExists(name string) (bool, error) {
	links, err := net.Interfaces()
	if err != nil {
		return false, fmt.Errorf("listing links: %w", err)
	}

	named := func(l net.Interface) bool { return l.Name == name }

	return slices.ContainsFunc(links, named), nil
}
