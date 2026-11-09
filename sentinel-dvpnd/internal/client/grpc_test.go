package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

func TestGRPCServer_StartGetWatchStopSession(t *testing.T) {
	t.Parallel()

	rt, home, state := newWorkingRuntime(t)

	state.SetLocalStats(service.PeerStats{RxBytes: 1_000_000})

	svc := startClientService(t, rt)

	req := newChannelRequest(home.NodeAddress.String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	startRes, err := svc.StartSession(ctx, req)
	require.NoError(t, err)
	require.Equal(t, client.SessionStatusActive, startRes.GetSession().GetStatus())
	require.Empty(t, startRes.GetSession().GetProxyEndpoint())

	waitTicked(t, ctx, home.DB, startRes.GetSession().GetId())

	getReq := &daemonv1.GetSessionRequest{Id: startRes.GetSession().GetId()}

	getRes, err := svc.GetSession(ctx, getReq)
	require.NoError(t, err)
	require.Equal(t, startRes.GetSession().GetId(), getRes.GetSession().GetId())

	watchReq := &daemonv1.WatchSessionRequest{Id: startRes.GetSession().GetId()}

	stream, err := svc.WatchSession(ctx, watchReq)
	require.NoError(t, err)

	first, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, startRes.GetSession().GetId(), first.GetSession().GetId())
	require.Equal(t, client.SessionStatusActive, first.GetSession().GetStatus())

	stopReq := &daemonv1.StopSessionRequest{}

	stopRes, err := svc.StopSession(ctx, stopReq)
	require.NoError(t, err)
	require.Equal(t, client.SessionStatusEnded, stopRes.GetSession().GetStatus())
	require.True(t, stopRes.GetKillSwitchReleased())

	// Teardown reports "closing" before "ended" (the app shows "Disconnecting" during the slow close).
	second, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, client.SessionStatusClosing, second.GetSession().GetStatus())

	third, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, client.SessionStatusEnded, third.GetSession().GetStatus())

	_, active := client.Current(rt)
	require.False(t, active)
}

func TestGRPCServer_StopSession_ByID(t *testing.T) {
	t.Parallel()

	rt, home, _ := newWorkingRuntime(t)
	svc := startClientService(t, rt)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	endedID := "previous-session"
	ended := &client.Session{
		ID:     endedID,
		Status: client.SessionStatusEnded,
	}
	require.NoError(t, home.DB.WithContext(ctx).Create(ended).Error)

	req := newChannelRequest(home.NodeAddress.String())
	startRes, err := svc.StartSession(ctx, req)
	require.NoError(t, err)

	activeID := startRes.GetSession().GetId()

	for _, id := range []string{"other", endedID} {
		_, err = svc.StopSession(ctx, &daemonv1.StopSessionRequest{Id: id})
		require.Equal(t, codes.NotFound, status.Code(err))

		current, active := client.Current(rt)
		require.True(t, active)
		require.Equal(t, activeID, current.ID)
	}

	stopRes, err := svc.StopSession(ctx, &daemonv1.StopSessionRequest{Id: activeID})
	require.NoError(t, err)
	require.Equal(t, activeID, stopRes.GetSession().GetId())
	require.Equal(t, client.SessionStatusEnded, stopRes.GetSession().GetStatus())

	_, active := client.Current(rt)
	require.False(t, active)
}

func TestGRPCServer_StopSession_ReleasesLeftoverKillSwitchByID(t *testing.T) {
	t.Parallel()

	fixture := newKillSwitchFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fixture.startAndSettle(t, ctx)
	require.NoError(t, fixture.rt.Stop(ctx))

	rt, afterRestart := restartRuntime(
		t, ctx, fixture.home, fixture.programClient, fixture.firewall, fixture.ks.lookup, &recordingDNSBackend{},
	)

	svc := startClientService(t, rt)

	_, err := svc.StopSession(ctx, &daemonv1.StopSessionRequest{Id: "x"})
	require.Equal(t, codes.NotFound, status.Code(err))

	require.Equal(t, 1, fixture.firewall.flushes())
	require.Empty(t, firewallRows(t, fixture.home))
	requireUnpinned(t, afterRestart)
}
