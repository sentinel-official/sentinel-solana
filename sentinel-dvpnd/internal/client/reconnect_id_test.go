package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
)

// After an automatic reconnect the caller still holds the id it started: it must read the live session and stop it.
func TestRuntime_AfterReconnectTheStartedIDFollowsTheLiveSession(t *testing.T) { //nolint:paralleltest // timing
	rt, home, _ := newIssuerFixture(t, 100_000, true)
	home.Loaded.Client.NodeTimeoutSeconds = 2
	home.NodeService.SetGetSessionErr(api.ErrInternal, 1000)
	row := startIssuer(t, rt, home)

	require.Eventually(t, func() bool { return home.NodeService.OpenedCount() >= 2 }, 8*time.Second, 10*time.Millisecond)
	home.NodeService.SetGetSessionErr(nil, 0) // the node answers again: the redialled session stays up

	var cur *client.Session
	require.Eventually(t, func() bool {
		c, ok := client.CurrentSession(rt)
		if ok && c.ID != row.ID && c.Status == client.SessionStatusActive {
			cur = c
		}

		return cur != nil
	}, 8*time.Second, 10*time.Millisecond, "a new session is up after the reconnect")

	got, err := client.GRPCGetSession(t.Context(), rt, row.ID)
	require.NoError(t, err)
	require.Equal(t, cur.ID, got.GetId(), "the started id reads the live session")
	require.Equal(t, client.SessionStatusActive, got.GetStatus())

	require.NoError(t, client.GRPCStopSession(t.Context(), rt, row.ID), "a stop by the started id stops the live session")

	_, stillUp := client.CurrentSession(rt)
	require.False(t, stillUp, "no session is left up")
}

// An id that never belonged to the current session is still refused.
func TestRuntime_StopSession_UnrelatedIDIsRefused(t *testing.T) { //nolint:paralleltest // timing
	rt, home, _ := newIssuerFixture(t, 100_000, false)
	startIssuer(t, rt, home)

	require.Error(t, client.GRPCStopSession(t.Context(), rt, "00000000-0000-0000-0000-000000000000"))

	_, up := client.CurrentSession(rt)
	require.True(t, up)
}
