package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/sentinel-official/sentinel-dvpnd/internal/store"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
)

const listSessionsLimit = 200

var errRecoveryPending = errors.New("the kill switch and sessions a previous run left are still being recovered")

type grpcServer struct {
	daemonv1.UnimplementedClientServiceServer

	runtime *Runtime
}

func (g *grpcServer) GetSession(
	ctx context.Context,
	req *daemonv1.GetSessionRequest,
) (*daemonv1.GetSessionResponse, error) {
	if s, ok := g.runtime.liveSession(req.GetId()); ok {
		return &daemonv1.GetSessionResponse{Session: toProtoSession(s)}, nil
	}

	var s Session
	if err := g.runtime.db.WithContext(ctx).First(&s, "id = ?", req.GetId()).Error; err != nil {
		return nil, daemonv1.ToStatus(store.WrapNotFound(err))
	}

	return &daemonv1.GetSessionResponse{Session: toProtoSession(&s)}, nil
}

func (g *grpcServer) ListChannels(
	ctx context.Context,
	_ *daemonv1.ListChannelsRequest,
) (*daemonv1.ListChannelsResponse, error) {
	var channels []Channel
	if err := g.runtime.db.WithContext(ctx).Find(&channels).Error; err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	response := &daemonv1.ListChannelsResponse{}
	for _, c := range channels {
		response.Channels = append(response.Channels, toProtoChannel(c))
	}

	return response, nil
}

func (g *grpcServer) ListSessions(
	ctx context.Context,
	_ *daemonv1.ListSessionsRequest,
) (*daemonv1.ListSessionsResponse, error) {
	var sessions []Session
	if err := g.runtime.db.WithContext(ctx).Order("started_unix desc").
		Limit(listSessionsLimit).Find(&sessions).Error; err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	// The DB row of the current session can lag its live state (a redial ends the old row before the redial
	// finishes): list what status says, so `client sessions` and `client status <id>` agree.
	if current, ok := g.runtime.current(); ok {
		for i := range sessions {
			if sessions[i].ID == current.ID {
				sessions[i] = *current
			}
		}
	}

	response := &daemonv1.ListSessionsResponse{}
	for i := range sessions {
		response.Sessions = append(response.Sessions, toProtoSession(&sessions[i]))
	}

	return response, nil
}

func (g *grpcServer) ListTickets(
	ctx context.Context,
	_ *daemonv1.ListTicketsRequest,
) (*daemonv1.ListTicketsResponse, error) {
	var tickets []Ticket
	if err := g.runtime.db.WithContext(ctx).Find(&tickets).Error; err != nil {
		return nil, daemonv1.ToStatus(err)
	}

	response := &daemonv1.ListTicketsResponse{}
	for _, t := range tickets {
		response.Tickets = append(response.Tickets, toProtoTicket(t))
	}

	return response, nil
}

func (g *grpcServer) StartSession(
	ctx context.Context,
	req *daemonv1.StartSessionRequest,
) (*daemonv1.StartSessionResponse, error) {
	if !g.runtime.recovered.Load() {
		return nil, daemonv1.ToStatus(fmt.Errorf("%w: %w", daemonv1.ErrUnavailable, errRecoveryPending))
	}

	s, err := g.runtime.startSession(ctx, req)

	switch {
	case errors.Is(err, errDaemonStopping):
		return nil, daemonv1.ToStatus(fmt.Errorf("%w: %w", daemonv1.ErrUnavailable, err))
	case err != nil:
		return nil, daemonv1.ToStatus(fmt.Errorf("%w: %w", daemonv1.ErrFailedPrecondition, err))
	}

	return &daemonv1.StartSessionResponse{Session: toProtoSession(s)}, nil
}

func (g *grpcServer) StopSession(
	ctx context.Context,
	req *daemonv1.StopSessionRequest,
) (*daemonv1.StopSessionResponse, error) {
	if !g.runtime.recovered.Load() {
		return nil, daemonv1.ToStatus(fmt.Errorf("%w: %w", daemonv1.ErrUnavailable, errRecoveryPending))
	}

	result, err := g.runtime.stopSessionByID(ctx, reasonStoppedByUser, req.GetId())

	switch {
	case errors.Is(err, errMissingSession):
		return nil, daemonv1.ToStatus(fmt.Errorf("%w: %w", daemonv1.ErrNotFound, err))
	case err != nil:
		return nil, daemonv1.ToStatus(err)
	}

	response := &daemonv1.StopSessionResponse{
		DnsRevertFailed:    result.dnsRevertFailed,
		KillSwitchReleased: result.killSwitchReleased,
	}
	if result.session != nil {
		response.Session = toProtoSession(result.session)
	}

	return response, nil
}

func (g *grpcServer) WatchSession(
	req *daemonv1.WatchSessionRequest,
	stream daemonv1.ClientService_WatchSessionServer,
) error {
	w, initial := g.runtime.watch(req.GetId())
	defer g.runtime.unwatch(w)

	if initial == nil && req.GetId() != "" {
		var stored Session
		if err := g.runtime.findFirst(stream.Context(), &stored, "id = ?", req.GetId()); err != nil {
			return daemonv1.ToStatus(store.WrapNotFound(err))
		}

		if stored.Status == SessionStatusEnded {
			initial = &stored
		}
	}

	if initial != nil {
		if err := stream.Send(&daemonv1.WatchSessionResponse{Session: toProtoSession(initial)}); err != nil {
			return fmt.Errorf("sending watch update: %w", err)
		}

		if initial.Status == SessionStatusEnded {
			return nil
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-w.wake:
		}

		s := w.takeLatest()
		if s == nil {
			continue
		}

		update := &daemonv1.WatchSessionResponse{Session: toProtoSession(s)}
		if err := stream.Send(update); err != nil {
			return fmt.Errorf("sending watch update: %w", err)
		}

		if s.Status == SessionStatusEnded {
			return nil
		}
	}
}

func toProtoChannel(c Channel) *daemonv1.Channel {
	return &daemonv1.Channel{
		Address:    c.Address,
		Cumulative: c.Cumulative,
		Escrow:     c.Escrow,
		Mode:       uint32(c.Mode),
		Node:       c.NodeAddress,
		OpenedAt:   c.OpenedUnix,
		Status:     c.Status,
	}
}

func toProtoSession(s *Session) *daemonv1.Session {
	response := &daemonv1.Session{
		Channel:       s.ChannelAddress,
		Error:         s.Error,
		Id:            s.ID,
		Node:          s.NodeAddress,
		ProxyEndpoint: s.ProxyEndpoint,
		Service:       s.Service,
		Spent:         s.SpentBaseUnits,
		StartedAt:     timestamppb.New(unixTime(s.StartedUnix)),
		Status:        s.Status,
		TicketHash:    s.TicketHash,
		Usage: &daemonv1.Usage{
			RxBytes: s.RxBytes,
			Seconds: s.Seconds,
			TxBytes: s.TxBytes,
		},
	}

	if s.EndedUnix != 0 {
		response.EndedAt = timestamppb.New(unixTime(s.EndedUnix))
	}

	return response
}

func toProtoTicket(t Ticket) *daemonv1.Ticket {
	return &daemonv1.Ticket{
		DataCapBytes: t.DataCapBytes,
		ExpiresAt:    t.ExpiresUnix,
		Hash:         t.Hash,
		Node:         t.NodeAddress,
		Provider:     t.ProviderAddress,
		Subscription: t.SubscriptionAddress,
	}
}

func unixTime(unix int64) time.Time {
	return time.Unix(unix, 0).UTC()
}
