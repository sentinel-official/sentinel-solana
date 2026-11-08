//go:build linux

package hysteria2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

// A Linux node that runs a host firewall (firewalld or ufw) drops inbound UDP to the hysteria2
// listen port unless the port is opened. The node opens its own port on Start and removes that
// opening on Stop — but only an opening it made itself. If the port is already open (an operator
// opened it, or it is permanent / in a zone), the node leaves it alone so Stop never deletes
// someone else's rule.
//
// ufw's `allow` is written under /etc/ufw and survives a reboot, so a crash that skips Stop would
// leave the opening behind. To bound that, the server records what it opened (backend + port) in a
// marker file and, on the next Start, closes and clears any opening a previous run left — which
// also cleans up after the listen port changes.
//
// Only firewalld and ufw are handled. On a host with neither active, the node logs that it is not
// touching the firewall: a bare nftables `accept` in its own table cannot override a drop in
// another base chain on the same input hook (e.g. a default-drop `inet filter` or iptables-nft
// INPUT policy), so adding one would give false assurance without actually letting traffic in.

const (
	fwNone      = ""
	fwFirewalld = "firewalld"
	fwUFW       = "ufw"

	firewallMarkerFile = "hy2-firewall.json"
)

// runFunc runs a command and returns its combined output. It is a seam so tests can stub the host
// firewall tools.
type runFunc func(ctx context.Context, name string, args ...string) ([]byte, error)

func defaultRun(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// openInboundUDP opens port/udp through the active host firewall and returns the backend it used.
// It returns fwNone (nothing for Stop to close) both when no managed firewall is active and when
// the port is already open — in the latter case the opening belongs to someone else, so Stop must
// not remove it.
func openInboundUDP(ctx context.Context, run runFunc, port uint16, log zerolog.Logger) (string, error) {
	p := strconv.Itoa(int(port))

	switch {
	case firewalldRunning(ctx, run):
		if firewalldPortOpen(ctx, run, p) {
			log.Info().Str("backend", fwFirewalld).Str("port", p).Msg("inbound udp already open; leaving it to the operator")

			return fwNone, nil
		}

		if out, err := run(ctx, "firewall-cmd", "--add-port="+p+"/udp"); err != nil {
			return fwNone, fmt.Errorf("firewalld: opening %s/udp: %w: %s", p, err, strings.TrimSpace(string(out)))
		}

		log.Info().Str("backend", fwFirewalld).Str("port", p).Msg("opened inbound udp")

		return fwFirewalld, nil
	case ufwActive(ctx, run):
		if ufwPortOpen(ctx, run, p) {
			log.Info().Str("backend", fwUFW).Str("port", p).Msg("inbound udp already open; leaving it to the operator")

			return fwNone, nil
		}

		if out, err := run(ctx, "ufw", "allow", p+"/udp"); err != nil {
			return fwNone, fmt.Errorf("ufw: opening %s/udp: %w: %s", p, err, strings.TrimSpace(string(out)))
		}

		log.Info().Str("backend", fwUFW).Str("port", p).Msg("opened inbound udp")

		return fwUFW, nil
	default:
		log.Warn().Str("port", p).Msg("no managed firewall (firewalld/ufw) active; not touching the host firewall — open " + p + "/udp yourself if you filter inbound")

		return fwNone, nil
	}
}

// closeInboundUDP removes an opening openInboundUDP made (backend != fwNone). fwNone means we opened
// nothing of our own, so there is nothing to remove.
func closeInboundUDP(ctx context.Context, run runFunc, backend string, port uint16, log zerolog.Logger) error {
	p := strconv.Itoa(int(port))

	switch backend {
	case fwFirewalld:
		if out, err := run(ctx, "firewall-cmd", "--remove-port="+p+"/udp"); err != nil {
			return fmt.Errorf("firewalld: closing %s/udp: %w: %s", p, err, strings.TrimSpace(string(out)))
		}
	case fwUFW:
		if out, err := run(ctx, "ufw", "delete", "allow", p+"/udp"); err != nil {
			return fmt.Errorf("ufw: closing %s/udp: %w: %s", p, err, strings.TrimSpace(string(out)))
		}
	case fwNone:
		return nil
	default:
		return fmt.Errorf("closing %s/udp: unknown firewall backend %q", p, backend)
	}

	log.Info().Str("backend", backend).Str("port", p).Msg("closed inbound udp")

	return nil
}

func firewalldRunning(ctx context.Context, run runFunc) bool {
	out, err := run(ctx, "firewall-cmd", "--state")

	return err == nil && strings.Contains(string(out), "running")
}

// firewalldPortOpen reports whether firewalld already allows port/udp. --query-port exits 0 and
// prints "yes" when the port is open, and exits non-zero (printing "no") when it is not.
func firewalldPortOpen(ctx context.Context, run runFunc, p string) bool {
	out, err := run(ctx, "firewall-cmd", "--query-port="+p+"/udp")

	return err == nil && strings.Contains(string(out), "yes")
}

func ufwActive(ctx context.Context, run runFunc) bool {
	out, err := run(ctx, "ufw", "status")

	return err == nil && strings.Contains(string(out), "Status: active")
}

// ufwPortOpen reports whether `ufw status` already lists an allow rule for exactly port/udp, so the
// node does not add (and later remove) a rule an operator put there.
func ufwPortOpen(ctx context.Context, run runFunc, p string) bool {
	out, err := run(ctx, "ufw", "status")
	if err != nil {
		return false
	}

	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == p+"/udp" && strings.EqualFold(f[1], "ALLOW") {
			return true
		}
	}

	return false
}

// firewallMarker records an opening this node made, so a later Start can undo it even if Stop never
// ran (a crash) or the listen port changed.
type firewallMarker struct {
	Backend string `json:"backend"`
	Port    uint16 `json:"port"`
}

func firewallMarkerPath(dir string) string { return filepath.Join(dir, firewallMarkerFile) }

func readFirewallMarker(dir string, log zerolog.Logger) (firewallMarker, bool) {
	b, err := os.ReadFile(firewallMarkerPath(dir))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Warn().Err(err).Str("file", firewallMarkerPath(dir)).Msg("reading firewall marker")
		}

		return firewallMarker{}, false
	}

	var m firewallMarker
	if err := json.Unmarshal(b, &m); err != nil || m.Backend == "" || m.Port == 0 {
		log.Warn().Err(err).Str("file", firewallMarkerPath(dir)).Msg("ignoring an unreadable firewall marker")

		return firewallMarker{}, false
	}

	return m, true
}

func writeFirewallMarker(dir string, m firewallMarker, log zerolog.Logger) {
	b, err := json.Marshal(m)
	if err != nil {
		log.Warn().Err(err).Msg("recording firewall opening")

		return
	}

	if err := os.WriteFile(firewallMarkerPath(dir), b, 0o600); err != nil {
		log.Warn().Err(err).Msg("recording firewall opening")
	}
}

func removeFirewallMarker(dir string, log zerolog.Logger) {
	if err := os.Remove(firewallMarkerPath(dir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn().Err(err).Msg("clearing firewall marker")
	}
}
