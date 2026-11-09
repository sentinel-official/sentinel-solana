package client

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
)

var errFirewallChanged = errors.New("firewall may have changed")

type firewallBackend interface {
	Apply(ctx context.Context, iface string, nodeAddrs []netip.Addr, rpcAndWSAddrs []netip.AddrPort, allowLAN bool) error
	Flush(ctx context.Context) error
}

type firewallState struct {
	kept    bool
	present bool
}

func (r *Runtime) applyFirewall(
	ctx context.Context,
	iface string,
	hosts nodeHosts,
	allowed allowlist,
	placeholder *connection,
) (firewallState, bool, error) {
	if !r.owns(placeholder) {
		return firewallState{}, false, ErrStoppedDuringConnect
	}

	actx, cancel := r.withTimeout(ctx)
	defer cancel()

	r.mu.Lock()
	prior := r.firewallState
	r.mu.Unlock()

	if err := r.firewallBackend.Apply(
		actx, iface, hosts.addrs, allowed.rpcAndWSAddrs, r.cfg.Network.AllowLAN,
	); err != nil {
		changed := errors.Is(err, errFirewallChanged)
		if changed {
			r.mu.Lock()
			r.firewallState.present = true
			r.mu.Unlock()
		}

		return prior, changed, fmt.Errorf("applying kill switch: %w", err)
	}

	r.mu.Lock()
	r.firewallState = firewallState{present: true}
	r.nodeHosts = hosts
	r.admitted = nil
	r.mu.Unlock()

	row := Firewall{
		Iface:         iface,
		NodeAddrs:     formatAddrs(hosts.addrs),
		NodeURI:       hosts.uri.String(),
		Persistent:    r.cfg.Network.KillSwitch == config.KillSwitchPersist,
		RPCAddrs:      formatAddrPorts(allowed.rpcAddrs),
		RPCURL:        r.rpcCfg.URL,
		RPCAndWSAddrs: formatAddrPorts(allowed.rpcAndWSAddrs),
		WSURL:         r.rpcCfg.WSURL,
	}
	replace := func(dbtx *gorm.DB) error {
		if err := dbtx.Where("1 = 1").Delete(&Firewall{}).Error; err != nil {
			return err
		}

		return dbtx.Create(&row).Error
	}

	if err := r.db.WithContext(actx).Transaction(replace); err != nil {
		return prior, true, fmt.Errorf("recording firewall: %w", err)
	}

	r.pin.Set(allowed.rpcAddrs)

	return prior, true, nil
}

// admitToBlock adds a node host to the kill switch in place, keeping everything it already admits
// (the block's node, the RPC, the tunnel interface), so a server switch can reach a node on another
// host without anything leaving outside the block. The next session's own block then replaces it
// with that node alone. The stored firewall row is left as it was: after a restart the block is
// rebuilt from it without the extra host, which fails closed.
func (r *Runtime) admitToBlock(ctx context.Context, host string, addrs []netip.Addr) error {
	var row Firewall
	if err := r.findFirst(ctx, &row); err != nil {
		return fmt.Errorf("reading the kill switch in place: %w", err)
	}

	nodeAddrs, rpcAndWSAddrs, _, err := parseFirewall(row)
	if err != nil {
		return fmt.Errorf("reading the kill switch in place: %w", err)
	}

	r.mu.Lock()
	extra := make([][]netip.Addr, 0, len(r.admitted)+2)
	extra = append(extra, nodeAddrs, addrs)
	for _, a := range r.admitted {
		extra = append(extra, a)
	}
	r.mu.Unlock()

	actx, cancel := r.withTimeout(ctx)
	defer cancel()

	if err := r.firewallBackend.Apply(actx, row.Iface, mergeAddrs(extra...), rpcAndWSAddrs, r.cfg.Network.AllowLAN); err != nil {
		if errors.Is(err, errFirewallChanged) {
			r.mu.Lock()
			r.firewallState.present = true
			r.mu.Unlock()
		}

		return fmt.Errorf("admitting node %s to the kill switch: %w", host, err)
	}

	r.mu.Lock()
	if r.admitted == nil {
		r.admitted = make(map[string][]netip.Addr)
	}
	r.admitted[host] = addrs
	r.mu.Unlock()

	return nil
}

func (r *Runtime) flushFirewall(ctx context.Context) error {
	fctx, cancelFlush := r.withTimeout(ctx)
	defer cancelFlush()

	if err := r.firewallBackend.Flush(fctx); err != nil {
		return fmt.Errorf("flushing kill switch: %w", err)
	}

	r.pin.Clear()

	dctx, cancelDelete := r.withTimeout(ctx)
	defer cancelDelete()

	if err := r.db.WithContext(dctx).Where("1 = 1").Delete(&Firewall{}).Error; err != nil {
		return err
	}

	r.mu.Lock()
	r.firewallState = firewallState{}
	r.nodeHosts = nodeHosts{}
	r.admitted = nil
	r.mu.Unlock()

	return nil
}

func (r *Runtime) releaseFirewall(ctx context.Context, leftover bool) (bool, error) {
	r.mu.Lock()
	state := r.firewallState
	r.mu.Unlock()

	if !state.present && !leftover {
		r.pin.Clear()

		return false, nil
	}

	if err := r.flushFirewall(context.WithoutCancel(ctx)); err != nil {
		r.mu.Lock()
		r.firewallState.present = true
		r.mu.Unlock()

		return false, err
	}

	return true, nil
}

func (r *Runtime) rollbackFirewall(ctx context.Context, prior firewallState) error {
	if prior.present && prior.kept {
		r.mu.Lock()
		r.firewallState = prior
		r.mu.Unlock()

		return nil
	}

	_, err := r.releaseFirewall(ctx, false)

	return err
}
