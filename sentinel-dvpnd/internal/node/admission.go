//go:build linux

package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/solana-foundation/solana-go/v2"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-go-sdk/v3/voucher"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const resultAccepted = "accepted"

var errSignerNotParty = errors.New("signer is not a party to the position")

type admission struct {
	credential   string
	enforcer     session.Enforcer
	expiresUnix  int64
	issuedTicket string
	position     *Position
	signer       string
	subject      string
	terms        session.Terms
	ticketHash   string
	voucher      *nodeapi.Voucher
}

func (r *Runtime) acceptVoucher(
	ctx context.Context,
	kind string,
	address solana.PublicKey,
	signer solana.PublicKey,
	submitted nodeapi.Voucher,
) (*Position, error) {
	position, err := r.positionForRequest(ctx, kind, address)
	if err != nil {
		return nil, err
	}

	if err := r.authorizeParty(ctx, position, signer); err != nil {
		return nil, err
	}

	if position.Mode != dvpn.PositionModeVoucher {
		return nil, api.ErrModeNotVoucher
	}

	if position.Kind == KindChannel && position.Status == PositionStatusClosing {
		chainCfg, _ := r.chainState()
		r.closeSessionsOfClosingPosition(ctx, position, chainCfg, time.Now().Unix())
	}

	if err := verifyVoucher(position, submitted); err != nil {
		return nil, err
	}

	if submitted.Cumulative > position.Escrow {
		return nil, api.ErrVoucherExceedsEscrow
	}

	if kind == KindChannel {
		r.meterMu.Lock()
		defer r.meterMu.Unlock()
	}

	if submitted.Cumulative > position.Cumulative {
		if err := r.storeVoucher(ctx, position, submitted, time.Now().Unix()); err != nil {
			return nil, err
		}

		r.updateMeter(ctx, position.Address, position.Cumulative)
		r.metrics.vouchers.WithLabelValues(kind, resultAccepted).Inc()
	}

	return position, nil
}

func (r *Runtime) admit(
	ctx context.Context,
	signer solana.PublicKey,
	req nodeapi.OpenSessionRequest,
) (*admission, error) {
	active, err := r.countActiveSessions(ctx)
	if err != nil {
		return nil, err
	}

	if active >= int64(r.cfg.Sessions.MaxPeers) {
		return nil, api.ErrPeerLimitReached
	}

	switch req.Credential.Type {
	case nodeapi.CredentialTypeChannel:
		if req.Credential.Channel == nil {
			return nil, api.ErrBadRequest
		}

		return r.admitChannel(ctx, signer, *req.Credential.Channel, req.Terms, req.Voucher)
	case nodeapi.CredentialTypeTicket:
		if req.Credential.Ticket == nil {
			return nil, api.ErrBadRequest
		}

		return r.admitTicket(ctx, signer, *req.Credential.Ticket)
	case nodeapi.CredentialTypeTicketRequest:
		if req.Credential.TicketRequest == nil {
			return nil, api.ErrBadRequest
		}

		issuedTicket, err := r.forwardTicketRequest(ctx, *req.Credential.TicketRequest)
		if err != nil {
			return nil, err
		}

		admitted, err := r.admitTicket(ctx, signer, issuedTicket)
		if err != nil {
			return nil, err
		}

		admitted.issuedTicket = issuedTicket

		return admitted, nil
	default:
		return nil, api.ErrBadRequest
	}
}

func (r *Runtime) admitChannel(
	ctx context.Context,
	signer solana.PublicKey,
	address solana.PublicKey,
	terms *session.Terms,
	submitted *nodeapi.Voucher,
) (*admission, error) {
	position, err := r.positionForRequest(ctx, KindChannel, address)
	if err != nil {
		return nil, err
	}

	if err := r.authorizeParty(ctx, position, signer); err != nil {
		return nil, err
	}

	_, node := r.chainState()
	switch {
	case position.Status != PositionStatusOpen:
		return nil, api.ErrChannelNotOpen
	case position.NodeOpenedUnix != node.OpenedAt:
		return nil, api.ErrChannelNodeStale
	}

	switch position.Mode {
	case dvpn.PositionModeStream:
		return r.admitStreamChannel(ctx, signer, position, submitted)
	case dvpn.PositionModeVoucher:
		return r.admitVoucherChannel(ctx, signer, position, terms, submitted, node)
	default:
		return nil, api.ErrBadRequest
	}
}

func (r *Runtime) admitStreamChannel(
	ctx context.Context,
	signer solana.PublicKey,
	position *Position,
	submitted *nodeapi.Voucher,
) (*admission, error) {
	if submitted != nil {
		return nil, api.ErrModeNotStream
	}

	if err := r.replaceSubjectSessions(ctx, position.Address); err != nil {
		return nil, fmt.Errorf("replacing active session: %w", err)
	}

	terms := session.Terms{
		Basis: session.BasisPerHour,
		Price: position.RatePerHour,
	}

	return &admission{
		credential: CredentialChannel,
		enforcer: session.NewStreamEnforcer(
			position.Escrow,
			position.StreamBase,
			position.RatePerHour,
			position.StreamStartedUnix,
		),
		signer:  signer.String(),
		subject: position.Address,
		terms:   terms,
	}, nil
}

func (r *Runtime) admitVoucherChannel(
	ctx context.Context,
	signer solana.PublicKey,
	position *Position,
	terms *session.Terms,
	submitted *nodeapi.Voucher,
	node dvpn.Node,
) (*admission, error) {
	if terms == nil || submitted == nil {
		return nil, api.ErrTermsRejected
	}

	if err := r.checkTerms(*terms, node); err != nil {
		return nil, err
	}

	if err := verifyVoucher(position, *submitted); err != nil {
		return nil, err
	}

	if submitted.Cumulative > position.Escrow {
		return nil, api.ErrVoucherExceedsEscrow
	}

	r.closeMu.Lock()
	defer r.closeMu.Unlock()

	if err := r.reloadSettled(ctx, position); err != nil {
		return nil, err
	}

	settled, err := r.projectedSettled(ctx, position)
	if err != nil {
		return nil, err
	}

	paid := max(position.Cumulative, submitted.Cumulative)

	meter := session.NewMeter(*terms, paid, settled)
	if paid < settled || meter.Headroom(session.Usage{}) <
		session.Owed(terms.Basis, terms.Price, r.minPrepayFor(terms.Basis))/2 {
		return nil, api.ErrPrepayBelowMinimum
	}

	if err := r.replaceSubjectSessionsLocked(ctx, position.Address); err != nil {
		return nil, fmt.Errorf("replacing active session: %w", err)
	}

	if err := r.reloadSettled(ctx, position); err != nil {
		return nil, err
	}

	meter = session.NewMeter(*terms, paid, position.Settled)

	return &admission{
		credential: CredentialChannel,
		enforcer:   meter,
		position:   position,
		signer:     signer.String(),
		subject:    position.Address,
		terms:      *terms,
		voucher:    submitted,
	}, nil
}

func (r *Runtime) authorizeParty(ctx context.Context, position *Position, signer solana.PublicKey) error {
	owner, err := solana.PublicKeyFromBase58(position.Owner)
	if err != nil {
		return fmt.Errorf("%w: %w", api.ErrUnauthorized, err)
	}

	address, err := solana.PublicKeyFromBase58(position.Address)
	if err != nil {
		return fmt.Errorf("%w: %w", api.ErrUnauthorized, err)
	}

	permission := program.PermUseChannel
	subjects := []solana.PublicKey{address, r.address}

	if position.Kind == KindLease {
		provider, err := solana.PublicKeyFromBase58(position.Provider)
		if err != nil {
			return fmt.Errorf("%w: %w", api.ErrUnauthorized, err)
		}

		permission = program.PermUseLease

		subjects = append(subjects, provider)
	}

	if err := auth.Authorize(ctx, r.programClient, owner, signer, permission, subjects...); err != nil {
		if errors.Is(err, auth.ErrUnauthorized) {
			return fmt.Errorf("%w: %w", api.ErrUnauthorized, errSignerNotParty)
		}

		return fmt.Errorf("authorizing %s: %w", signer, err)
	}

	return nil
}

func (r *Runtime) checkTerms(terms session.Terms, node dvpn.Node) error {
	if err := terms.Validate(); err != nil {
		return api.ErrTermsRejected
	}

	switch terms.Basis {
	case session.BasisPerGB:
		if terms.Price != node.ChannelPricePerGB {
			return api.ErrTermsRejected
		}
	case session.BasisPerHour:
		if terms.Price != node.ChannelPricePerHour {
			return api.ErrTermsRejected
		}
	}

	return nil
}

func (r *Runtime) hasActiveSession(ctx context.Context, subject string) (bool, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&Session{}).
		Where("subject = ? AND status = ?", subject, SessionStatusActive).Count(&count).Error; err != nil {
		return false, fmt.Errorf("counting active sessions: %w", err)
	}

	return count > 0, nil
}

func (r *Runtime) leaseHealthy(position *Position, node dvpn.Node, now int64) bool {
	if position.Status != PositionStatusOpen || position.NodeOpenedUnix != node.OpenedAt {
		return false
	}

	if position.Mode == dvpn.PositionModeStream {
		accrued := program.StreamAccrued(
			position.Escrow,
			position.StreamBase,
			position.RatePerHour,
			position.StreamStartedUnix,
			now,
		)

		return accrued < position.Escrow
	}

	elapsed := uint64(max(now-position.OpenedUnix, 0))
	owedElapsed := session.Owed(session.BasisPerHour, position.RatePerHour, elapsed)

	return position.RatePerHour == 0 || position.Cumulative > owedElapsed
}

func (r *Runtime) minPrepayFor(basis session.Basis) uint64 {
	if basis == session.BasisPerHour {
		return r.cfg.Sessions.MinPrepaySeconds
	}

	return r.cfg.Sessions.MinPrepayBytes
}

func (r *Runtime) projectedSettled(ctx context.Context, position *Position) (uint64, error) {
	var sessions []Session
	if err := r.db.WithContext(ctx).
		Where("subject = ? AND status = ?", position.Address, SessionStatusActive).Find(&sessions).Error; err != nil {
		return 0, fmt.Errorf("loading subject sessions: %w", err)
	}

	settled := position.Settled
	now := time.Now().Unix()

	for i := range sessions {
		sess := sessions[i]
		if stats, err := r.service.PeerStats(ctx, service.PeerID(sess.PeerID)); err == nil {
			foldStats(&sess, stats, now)
		}

		sess.Seconds = uint64(max(now-sess.CreatedUnix, 0))
		basis := session.Basis(sess.Basis)

		owed := session.Owed(basis, sess.Price, sess.Usage().Amount(basis))
		if settled < position.Cumulative {
			settled += min(owed, position.Cumulative-settled)
		}
	}

	return settled, nil
}

func (r *Runtime) reloadSettled(ctx context.Context, position *Position) error {
	var stored Position
	if err := r.db.WithContext(ctx).Select("settled").First(&stored, "address = ?", position.Address).Error; err != nil {
		return fmt.Errorf("loading position: %w", err)
	}

	position.Settled = stored.Settled

	return nil
}

func (r *Runtime) storeVoucher(ctx context.Context, position *Position, submitted nodeapi.Voucher, now int64) error {
	updates := map[string]any{
		"accepted_unix": now,
		"cumulative":    submitted.Cumulative,
		"signature":     submitted.Signature,
		"updated_unix":  now,
	}

	result := r.db.WithContext(ctx).Model(&Position{}).
		Where(map[string]any{
			"address":          position.Address,
			"kind":             position.Kind,
			"mode":             position.Mode,
			"node_opened_unix": position.NodeOpenedUnix,
			"opened_unix":      position.OpenedUnix,
		}).Where("cumulative < ?", submitted.Cumulative).Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("saving position: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		var stored Position
		if err := r.db.WithContext(ctx).First(&stored, "address = ?", position.Address).Error; err != nil {
			return fmt.Errorf("reloading position: %w", err)
		}

		if !samePosition(position, &stored) {
			return api.ErrChannelNotOpen
		}

		*position = stored

		return nil
	}

	position.AcceptedUnix = now
	position.Cumulative = submitted.Cumulative
	position.Signature = submitted.Signature
	position.UpdatedUnix = now

	return nil
}

func verifyVoucher(position *Position, submitted nodeapi.Voucher) error {
	voucherSigner, err := solana.PublicKeyFromBase58(position.VoucherSigner)
	if err != nil {
		return api.ErrInvalidVoucherSignature
	}

	address, _ := solana.PublicKeyFromBase58(position.Address)

	message := voucher.ChannelMessage(address, position.OpenedUnix, submitted.Cumulative)
	if position.Kind == KindLease {
		message = voucher.LeaseMessage(address, position.OpenedUnix, submitted.Cumulative)
	}

	if err := voucher.Verify(voucherSigner, message, submitted.Signature); err != nil {
		return api.ErrInvalidVoucherSignature
	}

	return nil
}
