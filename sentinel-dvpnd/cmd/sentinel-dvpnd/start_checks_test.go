package main_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	main "github.com/sentinel-official/sentinel-dvpnd/cmd/sentinel-dvpnd"
	"github.com/sentinel-official/sentinel-dvpnd/internal/preflight"
)

// A node whose RPC refuses it at start (down or 429) waits for the RPC instead of exiting.
func TestRunStartChecks_NodeWaitsOutAnUnreachableRPC(t *testing.T) {
	t.Parallel()

	calls := 0
	err := main.RunStartChecks(t.Context(), true, time.Millisecond, 4*time.Millisecond, func(context.Context) error {
		calls++
		if calls <= 3 {
			return preflight.ErrRPCUnreachable
		}

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 4, calls)
}

func TestRunStartChecks_ExitsOnOtherFailuresAndForTheClient(t *testing.T) {
	t.Parallel()

	other := errors.New("binary missing")
	calls := 0
	require.ErrorIs(t, main.RunStartChecks(t.Context(), true, time.Millisecond, time.Millisecond,
		func(context.Context) error { calls++; return other }), other)
	require.Equal(t, 1, calls, "only an unreachable RPC is waited for")

	calls = 0
	require.ErrorIs(t, main.RunStartChecks(t.Context(), false, time.Millisecond, time.Millisecond,
		func(context.Context) error { calls++; return preflight.ErrRPCUnreachable }), preflight.ErrRPCUnreachable)
	require.Equal(t, 1, calls, "the client exits at once, as before")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, main.RunStartChecks(ctx, true, time.Hour, time.Hour,
		func(context.Context) error { return preflight.ErrRPCUnreachable }), preflight.ErrRPCUnreachable)
}
