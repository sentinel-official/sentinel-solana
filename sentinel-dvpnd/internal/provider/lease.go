//go:build linux

package provider

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-programs/clients/go/core"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	actionFund          actionKind = "fund"
	actionOpen          actionKind = "open"
	actionStartClose    actionKind = "start_close"
	deadProbeIntervals             = 3
	staleProbeIntervals            = 2
)

type action struct {
	amount uint64
	kind   actionKind
}

type actionKind string

func (r *Runtime) applyAction(
	ctx context.Context, a action, nodeAddress solana.PublicKey, node dvpn.Node, lease *Lease,
) error {
	switch a.kind {
	case actionOpen:
		return r.openLease(ctx, a, nodeAddress, node)
	case actionFund:
		if err := r.fundLease(ctx, a, lease); err != nil {
			if errors.Is(err, errNodeChanged) {
				return r.applyAction(ctx, action{kind: actionStartClose}, nodeAddress, node, lease)
			}

			return err
		}

		return nil
	case actionStartClose:
		r.stopLeaseWatcher(lease.Address)

		if lease.Mode == dvpn.PositionModeStream {
			return r.closeStreamLease(ctx, lease)
		}

		return r.startCloseLease(ctx, lease)
	default:
		return nil
	}
}

func (r *Runtime) applyLeaseActions(
	ctx context.Context,
	probe Probe,
	nodeAddress solana.PublicKey,
	node dvpn.Node,
	lease *Lease,
	ratePerHour uint64,
) []error {
	var errs []error

	now := time.Now().Unix()
	for _, a := range leaseActions(lease, probe.Live && !probeStale(probe, now), probe.DeadSinceUnix,
		ratePerHour, r.cfg, now) {
		if err := r.applyAction(ctx, a, nodeAddress, node, lease); err != nil {
			r.log.Warn().Err(err).Str("node", probe.Node).Str("action", string(a.kind)).Msg("lease action failed")
			errs = append(errs, fmt.Errorf("%s lease action for node %s: %w", a.kind, probe.Node, err))
		}
	}

	return errs
}

func (r *Runtime) closeLeaseOfNode(ctx context.Context, nodeAddress solana.PublicKey, lease *Lease) error {
	if lease == nil || lease.Status != StatusOpen {
		return nil
	}

	return r.applyAction(ctx, action{kind: actionStartClose}, nodeAddress, dvpn.Node{}, lease)
}

func (r *Runtime) closeStreamLease(ctx context.Context, lease *Lease) error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()

	var stored Lease
	if err := r.db.WithContext(ctx).First(&stored, "address = ?", lease.Address).Error; err != nil {
		return fmt.Errorf("reading lease before close: %w", err)
	}

	if stored.Status != StatusOpen || stored.OpenedUnix != lease.OpenedUnix {
		return nil
	}

	address, err := solana.PublicKeyFromBase58(lease.Address)
	if err != nil {
		return fmt.Errorf("parsing lease address: %w", err)
	}

	leaseAccount, err := r.programClient.FetchLease(ctx, address)
	if err != nil {
		return fmt.Errorf("fetching stream lease: %w", err)
	}

	chainLease := leaseAccount.Value
	if chainLease.Mode != dvpn.PositionModeStream || chainLease.Status != dvpn.PositionStatusOpen ||
		chainLease.OpenedAt != lease.OpenedUnix {
		return nil
	}

	if err := r.sendCloseLease(ctx, address, chainLease); err != nil {
		return err
	}

	if err := r.db.WithContext(ctx).Model(&Lease{}).
		Where("address = ? AND opened_unix = ? AND status = ?", lease.Address, lease.OpenedUnix, StatusOpen).
		Updates(map[string]any{"healthy": false, "status": StatusClosed}).Error; err != nil {
		return fmt.Errorf("saving lease %s: %w", lease.Address, err)
	}

	return nil
}

func (r *Runtime) finalizeClosing(ctx context.Context) error {
	var closing []Lease
	if err := r.db.WithContext(ctx).Where("status = ?", StatusClosing).Find(&closing).Error; err != nil {
		return err
	}

	if len(closing) == 0 {
		return nil
	}

	now := time.Now().Unix()
	grace := int64(r.chainConfig().LeaseCloseGraceSeconds)

	for i := range closing {
		lease := &closing[i]
		r.stopLeaseWatcher(lease.Address)

		address, err := solana.PublicKeyFromBase58(lease.Address)
		if err != nil {
			continue
		}

		leaseAccount, err := r.programClient.FetchLease(ctx, address)
		if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
			if err := r.db.WithContext(ctx).Model(&Lease{}).Where("address = ?", lease.Address).
				Updates(map[string]any{"healthy": false, "status": StatusClosed}).Error; err != nil {
				r.log.Warn().Err(err).Str("lease", lease.Address).Msg("saving lease failed")
			}

			continue
		}

		if err != nil {
			r.log.Warn().Err(err).Str("lease", lease.Address).Msg("fetching lease failed")

			continue
		}

		chainLease := leaseAccount.Value
		if chainLease.Status != dvpn.PositionStatusClosing || now < chainLease.CloseStartedAt+grace {
			continue
		}

		if err := r.sendCloseLease(ctx, address, chainLease); err != nil {
			r.log.Warn().Err(err).Str("lease", lease.Address).Msg("closing lease failed")

			continue
		}

		if err := r.db.WithContext(ctx).Model(&Lease{}).Where("address = ?", lease.Address).
			Updates(map[string]any{"healthy": false, "status": StatusClosed}).Error; err != nil {
			r.log.Warn().Err(err).Str("lease", lease.Address).Msg("saving lease failed")
		}
	}

	return nil
}

func (r *Runtime) fundLease(ctx context.Context, a action, lease *Lease) error {
	if err := r.verifyLeaseNode(ctx, *lease); err != nil {
		return err
	}

	address, err := solana.PublicKeyFromBase58(lease.Address)
	if err != nil {
		return fmt.Errorf("parsing lease address: %w", err)
	}

	nodeAddress, err := solana.PublicKeyFromBase58(lease.Node)
	if err != nil {
		return fmt.Errorf("parsing lease node: %w", err)
	}

	grant, err := r.programClient.GrantAddress(
		ctx, r.owner, r.signer, dvpn.PermFundLeases, r.address, address, nodeAddress,
	)
	if err != nil {
		return err
	}

	ownerToken, err := r.programClient.AssociatedTokenAddress(r.owner)
	if err != nil {
		return err
	}

	vault, err := r.programClient.VaultAddress()
	if err != nil {
		return err
	}

	ix, err := dvpn.BuildFundLeasesInstruction(r.programClient.ProgramID, dvpn.FundLeasesParams{
		Amounts:           []uint64{a.amount},
		Grant:             grant,
		Mint:              r.programClient.Mint,
		OwnerToken:        ownerToken,
		Provider:          r.address,
		RemainingAccounts: r.programClient.RemainingAccounts(address),
		Signer:            r.signer,
		Vault:             vault,
	})
	if err != nil {
		return err
	}

	r.logLeaseFunding(ctx, a.amount, address, a.kind)
	_, err = r.sender.Send(ctx, r.privateKey, []solana.Instruction{ix}, tx.WithLabel("fund_leases"))

	return err
}

func (r *Runtime) keepLeases(ctx context.Context) error {
	r.keepMu.Lock()
	defer r.keepMu.Unlock()

	if err := r.syncLeases(ctx); err != nil {
		return err
	}

	if err := r.finalizeClosing(ctx); err != nil {
		return err
	}

	members, err := r.rosterNodeSet(ctx)
	if err != nil {
		return err
	}

	var probes []Probe
	if err := r.db.WithContext(ctx).Find(&probes).Error; err != nil {
		return err
	}

	now := time.Now().Unix()

	fresh := make(map[string]bool, len(probes))
	for _, probe := range probes {
		fresh[probe.Node] = !probeStale(probe, now)
	}

	for node := range members {
		if fresh[node] {
			continue
		}

		if err := r.probeNodes(ctx); err != nil {
			return err
		}

		if err := r.db.WithContext(ctx).Find(&probes).Error; err != nil {
			return err
		}

		break
	}

	var errs []error

	for _, probe := range probes {
		nodeAddress, err := solana.PublicKeyFromBase58(probe.Node)
		if err != nil {
			continue
		}

		var stored Lease

		var lease *Lease

		// Only an open lease counts. A closing one winds down on its own (finalizeClosing); if it
		// counted here, a node linked again during its close grace would get no lease for hours.
		err = r.db.WithContext(ctx).First(&stored, "node = ? AND status = ?", probe.Node, StatusOpen).Error
		if err == nil {
			lease = &stored
		} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			r.log.Warn().Err(err).Str("node", probe.Node).Msg("reading lease failed")
			errs = append(errs, fmt.Errorf("reading lease for node %s: %w", probe.Node, err))
		}

		if !members[probe.Node] {
			if err := r.closeLeaseOfNode(ctx, nodeAddress, lease); err != nil {
				r.log.Warn().Err(err).Str("node", probe.Node).Msg("closing lease of removed node failed")
				errs = append(errs, fmt.Errorf("closing lease of removed node %s: %w", probe.Node, err))
			}

			continue
		}

		nodeAccount, err := r.programClient.FetchNode(ctx, nodeAddress)
		if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
			if err := r.closeLeaseOfNode(ctx, nodeAddress, lease); err != nil {
				r.log.Warn().Err(err).Str("node", probe.Node).Msg("closing lease of closed node failed")
				errs = append(errs, fmt.Errorf("closing lease of closed node %s: %w", probe.Node, err))
			}

			continue
		}

		if err != nil {
			r.log.Warn().Err(err).Str("node", probe.Node).Msg("fetching node failed")
			errs = append(errs, fmt.Errorf("fetching node %s: %w", probe.Node, err))

			continue
		}

		node := nodeAccount.Value

		if lease != nil && lease.Status == StatusOpen && lease.NodeOpenedUnix != node.OpenedAt {
			if err := r.applyAction(ctx, action{kind: actionStartClose}, nodeAddress, node, lease); err != nil {
				r.log.Warn().Err(err).Str("lease", lease.Address).Msg("closing stale node lease failed")
				errs = append(errs, fmt.Errorf("closing stale lease %s: %w", lease.Address, err))
			}

			continue
		}

		ratePerHour := node.LeasePricePerHour
		if lease != nil {
			ratePerHour = lease.RatePerHour
		}

		errs = append(errs, r.applyLeaseActions(ctx, probe, nodeAddress, node, lease, ratePerHour)...)
	}

	return errors.Join(append(errs, r.scheduleLeaseWatchers(ctx))...)
}

func (r *Runtime) leasePrice(ctx context.Context, stored Lease, chainLease dvpn.Lease) (uint64, error) {
	if chainLease.Mode == dvpn.PositionModeStream {
		return chainLease.RatePerHour, nil
	}

	if stored.Status != "" && stored.OpenedUnix == chainLease.OpenedAt &&
		stored.NodeOpenedUnix == chainLease.NodeOpenedAt {
		return stored.RatePerHour, nil
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	node, err := r.programClient.FetchNode(ctx, chainLease.Node)
	if errors.Is(err, core.ErrAccountNotFound) || errors.Is(err, core.ErrOwnerMismatch) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("fetching lease price bound: %w", err)
	}

	if node.Value.OpenedAt != chainLease.NodeOpenedAt {
		return 0, nil
	}

	return node.Value.LeasePricePerHour, nil
}

func (r *Runtime) logLeaseFunding(ctx context.Context, amount uint64, address solana.PublicKey, kind actionKind) {
	providerAccount, err := r.programClient.FetchProvider(ctx, r.address)
	if err != nil {
		r.log.Warn().Err(err).Str("action", string(kind)).Str("lease", address.String()).
			Msg("fetching provider funding failed")

		return
	}

	balance := min(amount, providerAccount.Value.Balance)
	remaining := amount - balance
	revenue := min(remaining, providerAccount.Value.Revenue)
	wallet := remaining - revenue
	r.log.Info().Str("action", string(kind)).Str("lease", address.String()).
		Uint64("amount", amount).Uint64("balance", balance).Uint64("revenue", revenue).
		Uint64("wallet", wallet).Msg("planned lease funding")
}

func (r *Runtime) openLease(ctx context.Context, a action, nodeAddress solana.PublicKey, node dvpn.Node) error {
	var id [32]uint8
	if _, err := rand.Read(id[:]); err != nil {
		return fmt.Errorf("generating lease id: %w", err)
	}

	var terms dvpn.PositionTerms

	if r.cfg.Lease.Mode == config.LeaseModeStream {
		terms = &dvpn.PositionTermsStream{ExpectedRatePerHour: node.LeasePricePerHour}
	} else {
		terms = &dvpn.PositionTermsVoucher{VoucherSigner: r.signer}
	}

	address, _, err := dvpn.FindLeaseAddress(r.programClient.ProgramID, r.address, id)
	if err != nil {
		return fmt.Errorf("deriving lease address: %w", err)
	}

	grant, err := r.programClient.GrantAddress(ctx, r.owner, r.signer, dvpn.PermOpenLease, r.address, nodeAddress, address)
	if err != nil {
		return err
	}

	ownerToken, err := r.programClient.AssociatedTokenAddress(r.owner)
	if err != nil {
		return err
	}

	vault, err := r.programClient.VaultAddress()
	if err != nil {
		return err
	}

	rentPayer, err := r.sender.RentPayer(ctx, r.signer)
	if err != nil {
		return err
	}

	ix, err := dvpn.BuildOpenLeaseInstruction(r.programClient.ProgramID, dvpn.OpenLeaseParams{
		Escrow:             a.amount,
		Grant:              grant,
		LeaseID:            id,
		LeaseRentRecipient: r.signer,
		Mint:               r.programClient.Mint,
		Node:               nodeAddress,
		OwnerToken:         ownerToken,
		Provider:           r.address,
		RentPayer:          rentPayer,
		Signer:             r.signer,
		Terms:              terms,
		Vault:              vault,
	})
	if err != nil {
		return err
	}

	r.logLeaseFunding(ctx, a.amount, address, a.kind)

	if _, err := r.sender.Send(ctx, r.privateKey, []solana.Instruction{ix}, tx.WithLabel("open_lease")); err != nil {
		return err
	}

	leaseAccount, err := r.programClient.FetchLease(ctx, address)
	if err != nil {
		return fmt.Errorf("fetching opened lease %s: %w", address, err)
	}

	chainLease := leaseAccount.Value

	ratePerHour := node.LeasePricePerHour
	if chainLease.Mode == dvpn.PositionModeStream {
		ratePerHour = chainLease.RatePerHour
	}

	status := StatusOpen
	if chainLease.Status == dvpn.PositionStatusClosing {
		status = StatusClosing
	}

	row := Lease{
		Address:           address.String(),
		Cumulative:        chainLease.Redeemed,
		Escrow:            chainLease.Escrow,
		Mode:              chainLease.Mode,
		Node:              chainLease.Node.String(),
		NodeOpenedUnix:    chainLease.NodeOpenedAt,
		OpenedUnix:        chainLease.OpenedAt,
		RatePerHour:       ratePerHour,
		Redeemed:          chainLease.Redeemed,
		Status:            status,
		StreamBase:        chainLease.StreamBase,
		StreamStartedUnix: chainLease.StreamStartedAt,
		UpdatedUnix:       time.Now().Unix(),
	}

	row.Healthy = leaseHealthy(row.Status, row.Escrow, row.Redeemed, row.RatePerHour)
	if err := r.db.WithContext(ctx).Save(&row).Error; err != nil {
		return fmt.Errorf("saving lease %s: %w", row.Address, err)
	}

	return nil
}

func (r *Runtime) scheduleLeaseWatchers(ctx context.Context) error {
	members, err := r.rosterNodeSet(ctx)
	if err != nil {
		return err
	}

	var leases []Lease
	if err := r.db.WithContext(ctx).Find(&leases).Error; err != nil {
		return err
	}

	var probes []Probe
	if err := r.db.WithContext(ctx).Find(&probes).Error; err != nil {
		return err
	}

	byNode := make(map[string]Probe, len(probes))
	for _, probe := range probes {
		byNode[probe.Node] = probe
	}

	for _, lease := range leases {
		probe := byNode[lease.Node]
		if lease.Status == StatusOpen && lease.Mode == dvpn.PositionModeVoucher && probe.Live && members[lease.Node] {
			r.startLeaseWatcher(ctx, lease, probe)
		} else {
			r.stopLeaseWatcher(lease.Address)
		}
	}

	return nil
}

func (r *Runtime) sendCloseLease(ctx context.Context, address solana.PublicKey, chainLease dvpn.Lease) error {
	grant, err := r.programClient.GrantAddress(
		ctx, chainLease.Owner, r.signer, dvpn.PermCloseLease, r.address, address, chainLease.Node,
	)
	if err != nil {
		return err
	}

	node := chainLease.Node

	ix, err := dvpn.BuildCloseLeaseInstruction(r.programClient.ProgramID, dvpn.CloseLeaseParams{
		Grant:         grant,
		Lease:         address,
		Node:          &node,
		Provider:      r.address,
		RentRecipient: chainLease.RentRecipient,
		Signer:        r.signer,
	})
	if err != nil {
		return err
	}

	_, err = r.sender.Send(ctx, r.privateKey, []solana.Instruction{ix}, tx.WithLabel("close_lease"))

	return err
}

func (r *Runtime) startCloseLease(ctx context.Context, lease *Lease) error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()

	var stored Lease
	if err := r.db.WithContext(ctx).First(&stored, "address = ?", lease.Address).Error; err != nil {
		return fmt.Errorf("reading lease before close: %w", err)
	}

	if stored.Status != StatusOpen || stored.OpenedUnix != lease.OpenedUnix {
		return nil
	}

	address, err := solana.PublicKeyFromBase58(lease.Address)
	if err != nil {
		return fmt.Errorf("parsing lease address: %w", err)
	}

	nodeAddress, err := solana.PublicKeyFromBase58(lease.Node)
	if err != nil {
		return fmt.Errorf("parsing lease node: %w", err)
	}

	grant, err := r.programClient.GrantAddress(
		ctx, r.owner, r.signer, dvpn.PermStartCloseLease, r.address, address, nodeAddress,
	)
	if err != nil {
		return err
	}

	ix, err := dvpn.BuildStartCloseLeaseInstruction(r.programClient.ProgramID, dvpn.StartCloseLeaseParams{
		Grant:    grant,
		Lease:    address,
		Provider: r.address,
		Signer:   r.signer,
	})
	if err != nil {
		return err
	}

	_, err = r.sender.Send(ctx, r.privateKey, []solana.Instruction{ix}, tx.WithLabel("start_close_lease"))
	if err != nil {
		return err
	}

	lease.Status = StatusClosing
	if err := r.db.WithContext(ctx).Model(&Lease{}).
		Where("address = ? AND opened_unix = ? AND status = ?", lease.Address, lease.OpenedUnix, StatusOpen).
		Update("status", StatusClosing).Error; err != nil {
		return fmt.Errorf("saving lease %s: %w", lease.Address, err)
	}

	return nil
}

func (r *Runtime) syncLeases(ctx context.Context) error {
	leases, err := dvpn.FindLeaseAccounts(
		ctx, r.programClient.RPC, r.programClient.ProgramID, r.programClient.Options(), dvpn.LeaseProviderFilter(r.address),
	)
	if err != nil {
		return fmt.Errorf("listing leases: %w", err)
	}

	seen := map[string]bool{}
	now := time.Now().Unix()

	for _, keyed := range leases {
		chainLease := keyed.Value
		row := Lease{Address: keyed.Address.String()}

		seen[row.Address] = true
		if err := r.db.WithContext(ctx).First(&row, "address = ?", row.Address).Error; err != nil &&
			!errors.Is(err, gorm.ErrRecordNotFound) {
			r.log.Warn().Err(err).Str("lease", row.Address).Msg("reading lease failed")
		}

		ratePerHour, err := r.leasePrice(ctx, row, chainLease)
		if err != nil {
			return err
		}

		row.Escrow = chainLease.Escrow
		row.Mode = chainLease.Mode
		row.Node = chainLease.Node.String()
		row.NodeOpenedUnix = chainLease.NodeOpenedAt
		row.OpenedUnix = chainLease.OpenedAt
		row.Redeemed = chainLease.Redeemed
		row.StreamBase = chainLease.StreamBase
		row.StreamStartedUnix = chainLease.StreamStartedAt

		row.Cumulative = max(row.Cumulative, chainLease.Redeemed)
		row.RatePerHour = ratePerHour

		row.Status = StatusOpen
		if chainLease.Status == dvpn.PositionStatusClosing {
			row.Status = StatusClosing
		}

		row.Healthy = leaseHealthy(row.Status, chainLease.Escrow, chainLease.Redeemed, chainLease.RatePerHour)

		row.UpdatedUnix = now

		if err := r.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
			if err := db.Where("address = ?", row.Address).FirstOrCreate(&Lease{Address: row.Address}).Error; err != nil {
				return err
			}

			return db.Model(&Lease{}).Where("address = ?", row.Address).Updates(map[string]any{
				"escrow": row.Escrow, "mode": row.Mode, "node": row.Node, "node_opened_unix": row.NodeOpenedUnix,
				"opened_unix": row.OpenedUnix, "redeemed": row.Redeemed, "stream_base": row.StreamBase,
				"stream_started_unix": row.StreamStartedUnix, "status": row.Status, "healthy": row.Healthy,
				"updated_unix":  row.UpdatedUnix,
				"rate_per_hour": row.RatePerHour,
				"cumulative":    gorm.Expr("MAX(cumulative, ?)", chainLease.Redeemed),
				"signature":     gorm.Expr("CASE WHEN cumulative < ? THEN NULL ELSE signature END", chainLease.Redeemed),
			}).Error
		}); err != nil {
			return fmt.Errorf("syncing lease: %w", err)
		}
	}

	var stored []Lease
	if err := r.db.WithContext(ctx).Where("status <> ?", StatusClosed).Find(&stored).Error; err != nil {
		return err
	}

	for i := range stored {
		if seen[stored[i].Address] {
			continue
		}

		if err := r.db.WithContext(ctx).Model(&Lease{}).Where("address = ?", stored[i].Address).
			Updates(map[string]any{"status": StatusClosed, "healthy": false, "updated_unix": now}).Error; err != nil {
			r.log.Warn().Err(err).Str("lease", stored[i].Address).Msg("saving lease failed")
		}
	}

	var open int64
	if err := r.db.WithContext(ctx).Model(&Lease{}).Where("status = ?", StatusOpen).Count(&open).Error; err != nil {
		return err
	}

	r.metrics.leasesOpen.Set(float64(open))

	return nil
}

func leaseActions(
	lease *Lease,
	live bool,
	deadSinceUnix int64,
	ratePerHour uint64,
	cfg *config.Provider,
	now int64,
) []action {
	reserve := ratePerHour * cfg.Lease.ReserveHours

	if lease == nil {
		if live {
			return []action{
				{
					amount: reserve,
					kind:   actionOpen,
				},
			}
		}

		return nil
	}

	if lease.Status != StatusOpen {
		return nil
	}

	if !live {
		if deadSinceUnix != 0 && now-deadSinceUnix > int64(probeNodesInterval.Seconds())*deadProbeIntervals {
			return []action{{kind: actionStartClose}}
		}

		return nil
	}

	var planned []action

	switch lease.Mode {
	case dvpn.PositionModeStream:
		accrued := program.StreamAccrued(
			lease.Escrow, lease.StreamBase, lease.RatePerHour, lease.StreamStartedUnix, now,
		)
		if lease.Escrow-accrued < lease.RatePerHour*cfg.Lease.TopupHours {
			planned = append(planned, action{
				amount: reserve,
				kind:   actionFund,
			})
		}
	default:
		elapsed := uint64(max(now-lease.OpenedUnix, 0))

		owed := min(session.Owed(session.BasisPerHour, ratePerHour, elapsed), lease.Escrow)
		if lease.Escrow-owed < ratePerHour*cfg.Lease.TopupHours {
			planned = append(planned, action{
				amount: reserve,
				kind:   actionFund,
			})
		}
	}

	return planned
}

func leaseHealthy(status string, escrow, redeemed, ratePerHour uint64) bool {
	return status == StatusOpen && escrow-redeemed >= ratePerHour
}

func probeStale(probe Probe, now int64) bool {
	return probe.ProbedUnix == 0 || now-probe.ProbedUnix > staleProbeIntervals*int64(probeNodesInterval.Seconds())
}
