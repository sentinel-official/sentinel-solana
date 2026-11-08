package client_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
)

// idleConn dials a local listener through the runtime's pin: an idle keep-alive connection as the chain
// RPC or node API client leaves it in its pool.
func idleConn(t *testing.T, dial func() (net.Conn, error)) net.Conn {
	t.Helper()

	c, err := dial()
	require.NoError(t, err)

	return c
}

func localListener(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}

			t.Cleanup(func() { _ = c.Close() })
		}
	}()

	return ln
}

// A connection pooled before the tunnel came up keeps the physical source address; after tunnel-up its
// packets enter the tunnel with a source the peer drops, and the next request on it hangs until its
// timeout. Tunnel-up closes it, so the next request dials over the tunnel.
func TestRuntime_TunnelUp_DropsConnectionsPooledBeforeIt(t *testing.T) {
	t.Parallel()

	ln := localListener(t)
	rt, home, _ := newIssuerFixture(t, 100_000, false)

	pooled := idleConn(t, func() (net.Conn, error) { return home.Pin.DialContext(t.Context(), "tcp", ln.Addr().String()) })

	startIssuer(t, rt, home)

	_, err := pooled.Write([]byte("x"))
	require.ErrorIs(t, err, net.ErrClosed, "a connection opened before the tunnel is not reused after it")
}

// The same on the way down: a connection opened while the tunnel carried the route is dead once the
// tunnel is gone (the next session's first chain read waited out its timeout on one).
func TestRuntime_TunnelDown_DropsConnectionsPooledInTheTunnel(t *testing.T) {
	t.Parallel()

	ln := localListener(t)
	rt, home, _ := newIssuerFixture(t, 100_000, false)
	startIssuer(t, rt, home)

	pooled := idleConn(t, func() (net.Conn, error) { return home.Pin.DialContext(t.Context(), "tcp", ln.Addr().String()) })

	require.NoError(t, client.StopSession(t.Context(), rt))

	_, err := pooled.Write([]byte("x"))
	require.ErrorIs(t, err, net.ErrClosed, "a connection opened in the tunnel is not reused after it")
}
