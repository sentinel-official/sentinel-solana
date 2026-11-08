package cli

import (
	"context"
	"net"
	"time"
)

// pipeDialTimeout caps one dial of the daemon's endpoint. A running daemon accepts at once; a stopping or
// restarting one can leave its Windows pipe present but busy, and winio retries a busy pipe until the
// context ends (gRPC's connect timeout is 20 s), so without the cap a command hung that long.
const pipeDialTimeout = time.Second

// boundDial wraps dial so each attempt gives up after timeout.
func boundDial(timeout time.Duration, dial func(context.Context) (net.Conn, error)) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		return dial(ctx)
	}
}
