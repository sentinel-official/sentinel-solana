package client

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/rpcpin"
	"github.com/sentinel-official/sentinel-go-sdk/v3/amneziawg"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/openvpn"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/wireguard"
)

const recoverCloseTimeout = 5 * time.Second

var (
	ErrDNSCleanupFailed     = errors.New("dns cleanup failed")
	ErrServiceCleanupFailed = errors.New("service cleanup failed")
)

var errFirewallRowIncomplete = errors.New("persisted kill switch row is incomplete")

type Restoration struct {
	Leftover   bool
	Persistent bool
}

type storedAllowlist struct {
	hosts         nodeHosts
	rpcAndWSAddrs []netip.AddrPort
}

func (r *Runtime) Recover(ctx context.Context) error {
	stale, err := r.endStaleSessions(ctx)
	defer r.closeStaleRemoteSessions(ctx, stale)

	if err != nil {
		return fmt.Errorf("%w: %w", ErrServiceCleanupFailed, err)
	}

	if err := r.clearLeftovers(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrServiceCleanupFailed, err)
	}

	cctx, cancelCleanup := r.withTimeout(ctx)
	defer cancelCleanup()

	if err := r.remover.RemoveInterface(cctx, r.iface); err != nil {
		return fmt.Errorf("%w: %w", ErrServiceCleanupFailed, err)
	}

	if err := r.dnsBackend.Cleanup(cctx); err != nil {
		return fmt.Errorf("%w: %w", ErrDNSCleanupFailed, err)
	}

	defer r.recovered.Store(true)

	rctx, cancelRead := r.withTimeout(ctx)
	defer cancelRead()

	row, persistent, err := readFirewall(rctx, r.db)

	var unverified error

	switch {
	case err != nil:
		r.log.Error().Err(err).Msg(
			"reading the persisted kill switch; keeping whatever is in place until StopSession releases it",
		)
		r.keepFirewall()

		unverified = err
	case !persistent:
		if _, err := r.releaseFirewall(ctx, true); err != nil {
			r.log.Error().Err(err).Msg("releasing the kill switch a previous run left")
		}
	default:
		unverified = r.reapplyFirewall(ctx, row)
	}

	if row.Iface != "" && r.cfg.Network.DNSBackend != config.DNSNone &&
		row.DNSApplied {
		r.mu.Lock()
		r.dnsIface = row.Iface
		r.mu.Unlock()

		_ = r.revertDNS(ctx)
	}

	return unverified
}

func (r *Runtime) clearLeftovers(ctx context.Context) error {
	first := []service.Name{wireguard.Name, amneziawg.Name, openvpn.Name}
	rest := slices.DeleteFunc(service.Names(), func(name service.Name) bool { return slices.Contains(first, name) })
	names := slices.Concat(first, rest)
	dirs := make([]string, 0, len(names))

	var result error

	for _, name := range names {
		dirs = append(dirs, r.placement(name).ToolDir)

		// clearServiceLeftovers tears down a previous run's LIVE state: a stale tunnel process
		// (KillStale), the stale interface (the runner's Cleanup), an orphan adapter. A failure
		// there can leave a live tunnel up, so it stays fatal.
		if err := r.clearServiceLeftovers(ctx, name); err != nil {
			result = errors.Join(result, fmt.Errorf("clearing %s leftovers: %w", name, err))
		}
	}

	// Removing leftover tool-dir file COPIES is best-effort: a stale copy (e.g. root-owned, from
	// a prior privileged run, that a later non-root run cannot delete) is an old artifact, not a
	// live leak, and is overwritten on the next start, so it must not abort recovery.
	if err := r.removeCopies(dirs, r.iface); err != nil {
		r.log.Error().Err(err).Msg("removing leftover tool-dir copies; continuing daemon start")
	}

	return result
}

func (r *Runtime) clearServiceLeftovers(ctx context.Context, name service.Name) error {
	cfg, configured := r.services[name]
	if !configured {
		defaults, err := service.DefaultClientConfig(name)
		if errors.Is(err, service.ErrUnknownName) {
			return nil
		}

		cfg = defaults
	}

	serviceClient, err := service.NewClient(name, cfg, r.placement(name), r.log)
	if err != nil {
		return err
	}

	cleaner, ok := serviceClient.(service.ClientCleaner)
	if !ok {
		return nil
	}

	cctx, cancel := r.withTimeout(ctx)
	defer cancel()

	return cleaner.Cleanup(cctx)
}

func (r *Runtime) closeStaleRemoteSessions(ctx context.Context, stale []Session) {
	if len(stale) == 0 {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoverCloseTimeout)
		defer cancel()

		for _, row := range stale {
			if row.NodeAddress == "" || row.ID == "" {
				continue
			}

			address, err := solana.PublicKeyFromBase58(row.NodeAddress)
			if err != nil {
				r.log.Warn().Err(err).Str("session", row.ID).Msg("parsing stale node address")

				continue
			}

			node, err := r.programClient.FetchNode(ctx, address)
			if err != nil {
				r.log.Warn().Err(err).Str("session", row.ID).Msg("fetching stale session node")

				continue
			}

			dial, err := r.dialNode(ctx, address, node.Value)
			if err != nil {
				r.log.Warn().Err(err).Str("session", row.ID).Msg("dialing stale session node")

				continue
			}

			r.closeRemoteSessionWithin(ctx, dial.client, row.ID)
		}
	}()
}

func (r *Runtime) endStaleSessions(ctx context.Context) ([]Session, error) {
	lctx, cancelList := r.withTimeout(ctx)
	defer cancelList()

	var stale []Session
	if err := r.db.WithContext(lctx).Where("status <> ?", SessionStatusEnded).Find(&stale).Error; err != nil {
		r.log.Error().Err(err).Msg("listing the sessions a previous run left")

		return nil, fmt.Errorf("listing stale sessions: %w", err)
	}

	now := time.Now().Unix()

	var result error

	for i := range stale {
		s := &stale[i]
		s.Clean = false
		s.EndedUnix = now
		s.Error = reasonDaemonRestarted

		s.Status = SessionStatusEnded
		if err := r.save(ctx, s); err != nil {
			r.log.Warn().Err(err).Msg("recording a stale session")
			result = errors.Join(result, err)
		}
	}

	return stale, result
}

func (r *Runtime) keepFirewall() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.firewallState = firewallState{
		kept:    true,
		present: true,
	}
}

func (r *Runtime) reapplyFirewall(ctx context.Context, row Firewall) error {
	defer r.keepFirewall()

	stored, err := pinPersisted(row, r.rpcCfg, r.pin)
	if err != nil {
		r.log.Error().Err(err).Msg("persisted kill switch has unusable endpoints; StopSession releases it")

		return fmt.Errorf("persisted kill switch: %w", err)
	}

	r.mu.Lock()
	r.nodeHosts = stored.hosts
	r.mu.Unlock()

	if row.RPCURL != r.rpcCfg.URL || row.WSURL != r.rpcCfg.WSURL {
		r.log.Warn().
			Str("rpc_url", cli.RedactURL(r.rpcCfg.URL)).
			Str("stored_rpc_url", cli.RedactURL(row.RPCURL)).
			Str("stored_ws_url", cli.RedactURL(row.WSURL)).
			Str("ws_url", cli.RedactURL(r.rpcCfg.WSURL)).
			Msg("re-applying the persisted kill switch, which admits the rpc endpoints it was applied for, " +
				"not the configured ones; StopSession releases it")
	}

	actx, cancel := r.withTimeout(ctx)
	defer cancel()

	if err := r.firewallBackend.Apply(
		actx, row.Iface, stored.hosts.addrs, stored.rpcAndWSAddrs, r.cfg.Network.AllowLAN,
	); err != nil {
		r.log.Error().Err(err).Msg("re-applying the persisted kill switch; StopSession releases it")

		return fmt.Errorf("re-applying the persisted kill switch: %w", err)
	}

	return nil
}

func RestorePin(ctx context.Context, db *gorm.DB, rpcCfg cli.RPCConfig, pin *rpcpin.Pin) (Restoration, error) {
	exists, err := firewallTableExists(ctx, db)
	if err != nil {
		return Restoration{}, fmt.Errorf("store unusable: %w", err)
	}

	if !exists {
		return Restoration{}, nil
	}

	row, persistent, err := readFirewall(ctx, db)
	if err != nil {
		return Restoration{}, fmt.Errorf("store unusable: %w", err)
	}

	if !persistent {
		return Restoration{Leftover: row.ID != 0}, nil
	}

	if _, err := pinPersisted(row, rpcCfg, pin); err != nil {
		return Restoration{}, fmt.Errorf("persisted kill switch unusable: %w", err)
	}

	return Restoration{Persistent: true}, nil
}

func firewallTableExists(ctx context.Context, db *gorm.DB) (bool, error) {
	var count int64

	err := db.WithContext(ctx).
		Raw("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", Firewall{}.TableName()).
		Scan(&count).Error
	if err != nil {
		return false, fmt.Errorf("checking for the kill switch table: %w", err)
	}

	return count > 0, nil
}

func pinPersisted(row Firewall, rpcCfg cli.RPCConfig, pin *rpcpin.Pin) (storedAllowlist, error) {
	if row.RPCAndWSAddrs == "" || row.NodeAddrs == "" || row.NodeURI == "" {
		return storedAllowlist{}, errFirewallRowIncomplete
	}

	node, rpcAndWSAddrs, rpcAddrs, err := parseFirewall(row)
	if err != nil {
		return storedAllowlist{}, err
	}

	uri, err := api.ParsePeerURL(row.NodeURI)
	if err != nil {
		return storedAllowlist{}, fmt.Errorf("parsing kill switch node uri: %w", err)
	}

	if row.RPCURL == rpcCfg.URL {
		pin.Set(rpcAddrs)
	}

	hosts := nodeHosts{
		addrs: node,
		uri:   uri,
	}

	return storedAllowlist{
		hosts:         hosts,
		rpcAndWSAddrs: rpcAndWSAddrs,
	}, nil
}

func readFirewall(ctx context.Context, db *gorm.DB) (Firewall, bool, error) {
	var row Firewall

	switch err := db.WithContext(ctx).First(&row).Error; {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return Firewall{}, false, nil
	case err != nil:
		return Firewall{}, false, fmt.Errorf("reading the persisted kill switch: %w", err)
	}

	return row, row.Persistent, nil
}
