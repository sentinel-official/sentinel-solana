//go:build linux

package provider

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/providerapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

func (r *Runtime) checkTicketNode(ctx context.Context, request auth.SignedTicketRequest, sub dvpn.Subscription) error {
	var entry RosterNode
	if err := r.db.WithContext(ctx).First(
		&entry,
		"plan = ? AND node = ?",
		sub.Plan.String(),
		request.Request.Node.String(),
	).Error; err != nil {
		return api.ErrInvalidSubscription
	}

	var lease Lease
	if err := r.db.WithContext(ctx).First(
		&lease,
		"node = ? AND healthy = ?",
		request.Request.Node.String(),
		true,
	).Error; err != nil {
		return api.ErrLeaseNotFunded
	}

	return nil
}

func (r *Runtime) issueTicket(ctx context.Context, encoded string) (providerapi.IssueTicketResponse, error) {
	now := time.Now()

	request, sub, err := r.verifyTicketRequest(ctx, encoded, now)
	if err != nil {
		return providerapi.IssueTicketResponse{}, err
	}

	if err := r.checkTicketNode(ctx, request, sub); err != nil {
		return providerapi.IssueTicketResponse{}, err
	}

	nonce, err := auth.NewNonce()
	if err != nil {
		return providerapi.IssueTicketResponse{}, fmt.Errorf("generating nonce: %w", err)
	}

	expiresUnix := min(now.Unix()+int64(r.cfg.Tickets.MaxTTLSeconds), sub.ExpiresAt)

	ticket, err := r.reserveTicket(ctx, request, sub, nonce, now, expiresUnix)
	if err != nil {
		return providerapi.IssueTicketResponse{}, err
	}

	signed := auth.SignTicket(r.privateKey, ticket)
	r.metrics.tickets.WithLabelValues(resultIssued).Inc()

	return providerapi.IssueTicketResponse{
		DataCapBytes: ticket.DataCapBytes,
		ExpiresAt:    expiresUnix,
		Ticket:       signed.Encode(),
	}, nil
}

func (r *Runtime) reserveTicket(
	ctx context.Context,
	request auth.SignedTicketRequest,
	sub dvpn.Subscription,
	nonce auth.Nonce,
	now time.Time,
	expiresUnix int64,
) (auth.Ticket, error) {
	var ticket auth.Ticket

	err := r.db.WithContext(ctx).Transaction(func(dbtx *gorm.DB) error {
		row := Subscription{
			Address:    request.Request.Subscription.String(),
			OpenedUnix: sub.OpenedAt,
		}
		if err := dbtx.First(
			&row,
			"address = ? AND opened_unix = ?",
			row.Address,
			row.OpenedUnix,
		).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("loading subscription allocation: %w", err)
		}

		// Only tickets of THIS opening count: a cancelled and re-bought subscription has the same
		// address (PDA), and the old opening's unexpired tickets blocked a new server for up to a
		// ticket TTL.
		var active int64
		if err := dbtx.Model(&Ticket{}).Where(
			"subscription = ? AND expires_unix > ? AND issued_unix >= ?",
			row.Address,
			now.Unix(),
			sub.OpenedAt,
		).Count(&active).Error; err != nil {
			return err
		}

		if sub.MaxConnections != 0 && active >= int64(sub.MaxConnections) {
			return api.ErrPeerLimitReached
		}

		capacity := uint64(sub.DataCapGB) * session.BytesPerGB

		dataCap := ticketDataCap(capacity, row.AllocatedBytes, fairTicketCap(capacity, sub.MaxConnections, r.cfg.Tickets.DataCapBytes))
		if capacity != 0 && dataCap == 0 {
			return api.ErrInvalidTicket
		}

		ticket = auth.Ticket{
			DataCapBytes:   dataCap,
			ExpiresUnix:    expiresUnix,
			IssuedUnix:     now.Unix(),
			MaxConnections: sub.MaxConnections,
			Node:           request.Request.Node,
			Nonce:          nonce,
			Provider:       r.address,
			Subscription:   request.Request.Subscription,
			User:           sub.Owner,
			Version:        auth.TicketVersion,
		}
		digest := ticket.Hash()

		ticketRow := Ticket{
			DataCapBytes: dataCap,
			ExpiresUnix:  expiresUnix,
			Hash:         hex.EncodeToString(digest[:]),
			IssuedUnix:   now.Unix(),
			Node:         request.Request.Node.String(),
			Subscription: row.Address,
			User:         sub.Owner.String(),
		}
		if err := dbtx.Create(&ticketRow).Error; err != nil {
			return err
		}

		row.ActiveTickets = uint32(active + 1)
		row.AllocatedBytes += dataCap
		row.UpdatedUnix = now.Unix()

		return dbtx.Save(&row).Error
	})
	if err != nil {
		return auth.Ticket{}, fmt.Errorf("reserving ticket: %w", err)
	}

	return ticket, nil
}

func (r *Runtime) verifyTicketRequest(
	ctx context.Context,
	encoded string,
	now time.Time,
) (auth.SignedTicketRequest, dvpn.Subscription, error) {
	request, err := auth.DecodeTicketRequest(encoded)
	if err != nil || request.Request.Provider != r.address {
		return auth.SignedTicketRequest{}, dvpn.Subscription{}, api.ErrInvalidTicket
	}

	subscriptionAccount, err := r.programClient.FetchSubscription(ctx, request.Request.Subscription)
	if err != nil {
		return auth.SignedTicketRequest{}, dvpn.Subscription{}, api.ErrInvalidSubscription
	}

	sub := subscriptionAccount.Value
	if sub.Provider != r.address || now.Unix() >= sub.ExpiresAt {
		return auth.SignedTicketRequest{}, dvpn.Subscription{}, api.ErrInvalidSubscription
	}

	if err := auth.VerifyTicketRequest(ctx, r.programClient, request, sub.Owner, now, r.nonces); err != nil {
		return auth.SignedTicketRequest{}, dvpn.Subscription{}, fmt.Errorf("verifying ticket request: %w", err)
	}

	return request, sub, nil
}

// fairTicketCap: a ticket reserves its data cap from the plan until it expires, so with the
// configured per-ticket cap larger than capacity / max_connections the last connections the plan
// allows could never get a ticket (5 GB plan, 6 connections, 1 GB tickets: the 6th always got
// invalid_ticket). Each ticket is held to an equal share of the plan's data.
func fairTicketCap(capacity uint64, maxConnections uint8, cfgCap uint64) uint64 {
	if capacity == 0 || maxConnections == 0 {
		return cfgCap
	}

	share := max(capacity/uint64(maxConnections), 1)
	if cfgCap == 0 || cfgCap > share {
		return share
	}

	return cfgCap
}

func ticketDataCap(capacity, allocated, cfgCap uint64) uint64 {
	remaining := uint64(0)
	if capacity > allocated {
		remaining = capacity - allocated
	}

	if capacity != 0 && (cfgCap == 0 || cfgCap > remaining) {
		return remaining
	}

	return cfgCap
}
