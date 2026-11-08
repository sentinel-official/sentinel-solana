//go:build linux

package node_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/node"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
)

func sessionStatus(t *testing.T, home testutil.Home, id string) string {
	t.Helper()

	var stored node.Session
	require.NoError(t, home.DB.WithContext(t.Context()).First(&stored, "id = ?", id).Error)

	return stored.Status
}

func TestRuntime_CloseForClient_KeepsThePeerUntilTheReplyIsOut(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	node.SetCloseGrace(r, 300*time.Millisecond)
	sess := seedSession(t, home, "session-client-close", "peer-client-close")

	require.NoError(t, node.CloseForClient(t.Context(), r, sess))
	require.Equal(t, node.SessionStatusClosing, sessionStatus(t, home, sess.ID), "settled, peer kept for the reply")

	require.NoError(t, node.SyncUsage(t.Context(), r))
	require.Equal(t, node.SessionStatusClosing, sessionStatus(t, home, sess.ID), "not removed inside the grace")

	require.Eventually(t, func() bool {
		if err := node.SyncUsage(t.Context(), r); err != nil {
			t.Logf("sync usage: %v", err)
		}

		return sessionStatus(t, home, sess.ID) == node.SessionStatusClosed
	}, 5*time.Second, 50*time.Millisecond, "removed and closed after the grace")

	require.NoError(t, node.CloseForClient(t.Context(), r, sess), "a repeated close is a no-op")
}

func TestHandler_CloseSession_RepliesBeforeThePeerGoes(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	sess := seedSession(t, home, uuid.NewString(), "peer-handler-close")
	require.NoError(t, home.DB.WithContext(t.Context()).Model(sess).Update("signer", home.OwnerKey.PublicKey().String()).Error)
	client := newHandlerClient(t, startHandlerServer(t, r), home.OwnerKey)

	response, err := client.CloseSessionWithResponse(t.Context(), sess.ID)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode(), string(response.Body))
	require.Equal(t, node.SessionStatusClosing, sessionStatus(t, home, sess.ID),
		"the 204 leaves while the peer, and so the reply route, is still there")
}
