//go:build linux

package node

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/solana-foundation/solana-go/v2"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/version"
)

type handler struct {
	nodeapi.Unimplemented

	runtime *Runtime
}

func newHandler(r *Runtime) *handler {
	return &handler{runtime: r}
}

func (h *handler) CloseSession(
	ctx context.Context,
	req nodeapi.CloseSessionRequestObject,
) (nodeapi.CloseSessionResponseObject, error) {
	signer, err := api.RequireSigner(ctx)
	if err != nil {
		return nil, err
	}

	sess, err := h.runtime.sessionFor(ctx, req.ID, signer)
	if err != nil {
		return nil, err
	}

	if err := h.runtime.closeForClient(ctx, sess); err != nil {
		return nil, err
	}

	return nodeapi.CloseSession204Response{}, nil
}

func (h *handler) GetChannel(
	ctx context.Context,
	req nodeapi.GetChannelRequestObject,
) (nodeapi.GetChannelResponseObject, error) {
	signer, err := api.RequireSigner(ctx)
	if err != nil {
		return nil, err
	}

	position, err := h.runtime.refreshPosition(ctx, KindChannel, req.Channel)
	if err != nil {
		return nil, err
	}

	if err := h.runtime.authorizeParty(ctx, position, signer); err != nil {
		return nil, err
	}

	view, err := h.runtime.positionView(ctx, position)
	if err != nil {
		return nil, err
	}

	return nodeapi.GetChannel200JSONResponse(view), nil
}

func (h *handler) GetHealth(
	context.Context,
	nodeapi.GetHealthRequestObject,
) (nodeapi.GetHealthResponseObject, error) {
	return nodeapi.GetHealth200JSONResponse{
		Status:  "ok",
		Version: version.Tag,
	}, nil
}

func (h *handler) GetLease(
	ctx context.Context,
	req nodeapi.GetLeaseRequestObject,
) (nodeapi.GetLeaseResponseObject, error) {
	signer, err := api.RequireSigner(ctx)
	if err != nil {
		return nil, err
	}

	position, err := h.runtime.refreshPosition(ctx, KindLease, req.Lease)
	if err != nil {
		return nil, err
	}

	if err := h.runtime.authorizeParty(ctx, position, signer); err != nil {
		return nil, err
	}

	view, err := h.runtime.positionView(ctx, position)
	if err != nil {
		return nil, err
	}

	return nodeapi.GetLease200JSONResponse(view), nil
}

func (h *handler) GetLeaseEvents(
	ctx context.Context,
	req nodeapi.GetLeaseEventsRequestObject,
) (nodeapi.GetLeaseEventsResponseObject, error) {
	signer, err := api.RequireSigner(ctx)
	if err != nil {
		return nil, err
	}

	position, err := h.runtime.refreshPosition(ctx, KindLease, req.Lease)
	if err != nil {
		return nil, err
	}

	if err := h.runtime.authorizeParty(ctx, position, signer); err != nil {
		return nil, err
	}

	body := h.runtime.openEventStream(ctx, position.Address)

	var current Position
	if err := h.runtime.db.WithContext(ctx).First(&current, "address = ?", position.Address).Error; err != nil {
		if closer, ok := body.(io.Closer); ok {
			_ = closer.Close()
		}

		return nil, fmt.Errorf("reloading lease events position: %w", err)
	}

	if current.Status != PositionStatusOpen {
		reason := nodeapi.ReasonPositionClosing
		if current.Status == PositionStatusClosed {
			reason = nodeapi.ReasonPositionClosed
		}

		if err := h.runtime.publishClosed(current.Address, reason); err != nil {
			if closer, ok := body.(io.Closer); ok {
				_ = closer.Close()
			}

			return nil, err
		}
	}

	return nodeapi.EventsResponse{Body: body}, nil
}

func (h *handler) GetMetadata(
	context.Context,
	nodeapi.GetMetadataRequestObject,
) (nodeapi.GetMetadataResponseObject, error) {
	return nodeapi.GetMetadata200JSONResponse(h.runtime.metadata.Current()), nil
}

func (h *handler) GetSession(
	ctx context.Context,
	req nodeapi.GetSessionRequestObject,
) (nodeapi.GetSessionResponseObject, error) {
	signer, err := api.RequireSigner(ctx)
	if err != nil {
		return nil, err
	}

	sess, err := h.runtime.sessionFor(ctx, req.ID, signer)
	if err != nil {
		return nil, err
	}

	view, err := h.runtime.reloadSessionView(ctx, sess)
	if err != nil {
		return nil, err
	}

	return nodeapi.GetSession200JSONResponse{
		CloseReason: nodeapi.ClosedReason(sess.CloseReason),
		Consumption: view.Consumption,
		Ledger:      view.Ledger,
		Status:      nodeapi.SessionStatus(sess.Status),
	}, nil
}

func (h *handler) GetSessionEvents(
	ctx context.Context,
	req nodeapi.GetSessionEventsRequestObject,
) (nodeapi.GetSessionEventsResponseObject, error) {
	signer, err := api.RequireSigner(ctx)
	if err != nil {
		return nil, err
	}

	sess, err := h.runtime.sessionFor(ctx, req.ID, signer)
	if err != nil {
		return nil, err
	}

	body := h.runtime.openEventStream(ctx, sess.ID)

	sess, err = h.runtime.sessionFor(ctx, req.ID, signer)
	if err != nil {
		if closer, ok := body.(io.Closer); ok {
			_ = closer.Close()
		}

		return nil, err
	}

	if sess.Status != SessionStatusActive {
		if err := h.runtime.publishClosed(sess.ID, sess.CloseReason); err != nil {
			if closer, ok := body.(io.Closer); ok {
				_ = closer.Close()
			}

			return nil, err
		}
	}

	return nodeapi.EventsResponse{Body: body}, nil
}

func (h *handler) OpenSession(
	ctx context.Context,
	req nodeapi.OpenSessionRequestObject,
) (nodeapi.OpenSessionResponseObject, error) {
	signer, err := api.RequireSigner(ctx)
	if err != nil {
		return nil, err
	}

	h.runtime.admitMu.Lock()
	defer h.runtime.admitMu.Unlock()

	admitted, err := h.runtime.admit(ctx, signer, *req.Body)
	if err != nil {
		return nil, err
	}

	sess, peer, err := h.runtime.openSession(ctx, admitted, req.Body.Peer)
	if err != nil {
		if errors.Is(err, service.ErrPeerExists) {
			return nil, api.ErrConflict
		}

		return nil, err
	}

	view, err := h.runtime.reloadSessionView(ctx, sess)
	if err != nil {
		return nil, err
	}

	response := nodeapi.OpenSession200JSONResponse{
		Consumption: view.Consumption,
		Ledger:      view.Ledger,
		Peer:        peer,
		SessionID:   sess.ID,
	}
	if admitted.expiresUnix != 0 {
		response.ExpiresAt = &admitted.expiresUnix
	}

	if admitted.issuedTicket != "" {
		response.Ticket = &admitted.issuedTicket
	}

	return response, nil
}

func (h *handler) SendChannelVoucher(
	ctx context.Context,
	req nodeapi.SendChannelVoucherRequestObject,
) (nodeapi.SendChannelVoucherResponseObject, error) {
	signer, err := api.RequireSigner(ctx)
	if err != nil {
		return nil, err
	}

	position, err := h.runtime.acceptVoucher(ctx, KindChannel, req.Channel, signer, req.Body.Voucher)
	if err != nil {
		return nil, err
	}

	view, err := h.runtime.positionView(ctx, position)
	if err != nil {
		return nil, err
	}

	return nodeapi.SendChannelVoucher200JSONResponse(view), nil
}

func (h *handler) SendLeaseVoucher(
	ctx context.Context,
	req nodeapi.SendLeaseVoucherRequestObject,
) (nodeapi.SendLeaseVoucherResponseObject, error) {
	signer, err := api.RequireSigner(ctx)
	if err != nil {
		return nil, err
	}

	position, err := h.runtime.acceptVoucher(ctx, KindLease, req.Lease, signer, req.Body.Voucher)
	if err != nil {
		return nil, err
	}

	view, err := h.runtime.positionView(ctx, position)
	if err != nil {
		return nil, err
	}

	return nodeapi.SendLeaseVoucher200JSONResponse(view), nil
}

func (r *Runtime) sessionFor(ctx context.Context, id string, signer solana.PublicKey) (*Session, error) {
	var sess Session
	if err := r.db.WithContext(ctx).First(&sess, "id = ?", id).Error; err != nil {
		return nil, api.ErrSessionNotFound
	}

	if sess.Signer != signer.String() {
		return nil, api.ErrUnauthorized
	}

	return &sess, nil
}
