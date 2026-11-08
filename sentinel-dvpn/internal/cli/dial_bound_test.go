package cli_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpn/internal/cli"
)

// A command run while the daemon restarted hung for many seconds on Windows: the pipe was present but busy,
// and the dial retried it until gRPC's 20 s connect timeout. Each dial attempt is now capped.
func TestBoundDial_GivesUpOnABusyEndpoint(t *testing.T) {
	t.Parallel()

	busy := func(ctx context.Context) (net.Conn, error) {
		<-ctx.Done() // a busy pipe: winio retries until the context ends

		return nil, ctx.Err()
	}

	done := make(chan error, 1)
	start := time.Now()

	go func() {
		_, err := cli.BoundDial(100*time.Millisecond, busy)(context.Background(), "")
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(start), time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("the dial of a busy endpoint did not give up")
	}
}

func TestBoundDial_PassesAConnectionThrough(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	t.Cleanup(func() { require.NoError(t, client.Close()); require.NoError(t, server.Close()) })

	got, err := cli.BoundDial(time.Second, func(context.Context) (net.Conn, error) { return client, nil })(t.Context(), "")
	require.NoError(t, err)
	require.Equal(t, client, got)
	require.Equal(t, time.Second, cli.PipeDialTimeout)
}
