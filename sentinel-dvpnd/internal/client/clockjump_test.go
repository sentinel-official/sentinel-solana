package client_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
)

// The system time stepping forward (NTP, a manual change, resume) by more than the node timeout must
// not end a live session as "node timed out": the timeout counts elapsed time, not wall seconds.
func TestRuntime_IssueLoop_WallClockJumpDoesNotTimeOutTheNode(t *testing.T) { //nolint:paralleltest // swaps the wall clock
	rt, home, _ := newIssuerFixture(t, 100_000, false)
	row := startIssuer(t, rt, home)

	client.SetWallClockOffset(t, 150*time.Second)
	time.Sleep(3 * time.Second) // several sample and poll ticks on the jumped clock

	var now client.Session
	require.NoError(t, home.DB.WithContext(t.Context()).First(&now, "id = ?", row.ID).Error)
	require.Equal(t, client.SessionStatusActive, now.Status, "the session stays up across a clock jump (error %q)", now.Error)
}
