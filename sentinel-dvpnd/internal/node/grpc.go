//go:build linux

package node

import (
	"context"
	"time"

	"github.com/sentinel-official/sentinel-dvpnd/internal/store"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	listRedemptionsLimit = 200
	listSessionsLimit    = 500
	redeemReceiptLimit   = 20
)

type grpcServer struct {
	daemonv1.UnimplementedNodeServiceServer

	runtime *Runtime
}

func (g *grpcServer) CloseNodeSession(
	ctx context.Context,
	req *daemonv1.CloseNodeSessionRequest,
) (*daemonv1.CloseNodeSessionResponse, error) {
	var sess Session
	if err := g.runtime.db.WithContext(ctx).First(&sess, "id = ?", req.GetId()).Error; err != nil {
		return nil, daemonv1.ToStatus(store.WrapNotFound(err))
	}

	if err := g.runtime.closeSession(ctx, &sess, nodeapi.ReasonOperator); err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	return &daemonv1.CloseNodeSessionResponse{}, nil
}

func (g *grpcServer) GetNodeInfo(
	ctx context.Context,
	_ *daemonv1.GetNodeInfoRequest,
) (*daemonv1.GetNodeInfoResponse, error) {
	chainCfg, node := g.runtime.chainState()

	active, err := g.runtime.countActiveSessions(ctx)
	if err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	info := &daemonv1.NodeInfo{
		Address:  g.runtime.address.String(),
		Id:       g.runtime.cfg.ID,
		ListedAt: node.ListedAt,
		Live:     program.Live(node, chainCfg, time.Now().Unix()),
		Operator: g.runtime.signer.String(),
		Owner:    g.runtime.owner.String(),
		Peers:    uint32(active),
		Prices: &daemonv1.Prices{
			ChannelPricePerGb:   node.ChannelPricePerGB,
			ChannelPricePerHour: node.ChannelPricePerHour,
			LeasePricePerHour:   node.LeasePricePerHour,
		},
		Service: g.runtime.cfg.Service,
		Status:  formatListingStatus(node.Status),
	}

	return &daemonv1.GetNodeInfoResponse{Info: info}, nil
}

func (g *grpcServer) GetNodeSession(
	ctx context.Context,
	req *daemonv1.GetNodeSessionRequest,
) (*daemonv1.GetNodeSessionResponse, error) {
	var sess Session
	if err := g.runtime.db.WithContext(ctx).First(&sess, "id = ?", req.GetId()).Error; err != nil {
		return nil, daemonv1.ToStatus(store.WrapNotFound(err))
	}

	return &daemonv1.GetNodeSessionResponse{Session: toProtoSession(sess)}, nil
}

func (g *grpcServer) HeartbeatNow(
	ctx context.Context,
	_ *daemonv1.HeartbeatNowRequest,
) (*daemonv1.HeartbeatNowResponse, error) {
	if err := g.runtime.sendHeartbeat(ctx); err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	return &daemonv1.HeartbeatNowResponse{}, nil
}

func (g *grpcServer) ListNodeSessions(
	ctx context.Context,
	_ *daemonv1.ListNodeSessionsRequest,
) (*daemonv1.ListNodeSessionsResponse, error) {
	var sessions []Session
	if err := g.runtime.db.WithContext(ctx).
		Order("created_unix desc").Limit(listSessionsLimit).Find(&sessions).Error; err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	response := &daemonv1.ListNodeSessionsResponse{}
	for _, sess := range sessions {
		response.Sessions = append(response.Sessions, toProtoSession(sess))
	}

	return response, nil
}

func (g *grpcServer) ListPositions(
	ctx context.Context,
	_ *daemonv1.ListPositionsRequest,
) (*daemonv1.ListPositionsResponse, error) {
	var positions []Position
	if err := g.runtime.db.WithContext(ctx).Find(&positions).Error; err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	response := &daemonv1.ListPositionsResponse{}
	for _, position := range positions {
		response.Positions = append(response.Positions, toProtoPosition(position))
	}

	return response, nil
}

func (g *grpcServer) ListRedemptions(
	ctx context.Context,
	_ *daemonv1.ListRedemptionsRequest,
) (*daemonv1.ListRedemptionsResponse, error) {
	var rows []Redemption
	if err := g.runtime.db.WithContext(ctx).
		Order("sent_unix desc").Limit(listRedemptionsLimit).Find(&rows).Error; err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	response := &daemonv1.ListRedemptionsResponse{}
	for _, row := range rows {
		response.Redemptions = append(response.Redemptions, toProtoRedemption(row))
	}

	return response, nil
}

func (g *grpcServer) RedeemNow(
	ctx context.Context,
	_ *daemonv1.RedeemNowRequest,
) (*daemonv1.RedeemNowResponse, error) {
	if err := g.runtime.redeemNow(ctx); err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	var rows []Redemption
	if err := g.runtime.db.WithContext(ctx).Where("status = ?", RedemptionStatusConfirmed).
		Order("sent_unix desc").Limit(redeemReceiptLimit).Find(&rows).Error; err != nil {
		g.runtime.log.Warn().Err(err).Msg("loading confirmed redemptions")
	}

	response := &daemonv1.RedeemNowResponse{}
	for _, row := range rows {
		response.Signatures = append(response.Signatures, row.Signature)
	}

	return response, nil
}

func (g *grpcServer) SyncConfigNow(
	ctx context.Context,
	_ *daemonv1.SyncConfigNowRequest,
) (*daemonv1.SyncConfigNowResponse, error) {
	if err := g.runtime.syncConfig(ctx); err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	if err := g.runtime.refreshMetadata(ctx); err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	return &daemonv1.SyncConfigNowResponse{}, nil
}

func formatListingStatus(status uint8) string {
	if status == dvpn.ListingStatusListed {
		return "listed"
	}

	return "delisted"
}

func toProtoPosition(position Position) *daemonv1.Position {
	return &daemonv1.Position{
		Address:    position.Address,
		Cumulative: position.Cumulative,
		Escrow:     position.Escrow,
		Kind:       position.Kind,
		Mode:       uint32(position.Mode),
		OpenedAt:   position.OpenedUnix,
		Owner:      position.Owner,
		Redeemed:   position.Redeemed,
		Status:     position.Status,
	}
}

func toProtoRedemption(row Redemption) *daemonv1.Redemption {
	return &daemonv1.Redemption{
		ConfirmedAt: row.ConfirmedUnix,
		Error:       row.Error,
		Kind:        row.Kind,
		SentAt:      row.SentUnix,
		Signature:   row.Signature,
		Status:      row.Status,
		Subjects:    parseSubjects(row.Subjects),
	}
}

func toProtoSession(sess Session) *daemonv1.NodeSession {
	return &daemonv1.NodeSession{
		CreatedAt:  sess.CreatedUnix,
		Credential: sess.Credential,
		Id:         sess.ID,
		PeerId:     sess.PeerID,
		Status:     sess.Status,
		Subject:    sess.Subject,
		UpdatedAt:  sess.UpdatedUnix,
		Usage: &daemonv1.Usage{
			RxBytes: sess.RxBytes,
			Seconds: sess.Seconds,
			TxBytes: sess.TxBytes,
		},
	}
}
