package rpcpin_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/rpcpin"
)

// A pooled RPC connection can outlive the tunnel it was opened over (macOS, WireGuard <-> AmneziaWG switch).
// CloseConns closes every connection the pin dialled, and only once each.
func TestPin_CloseConns_ClosesWhatItDialled(t *testing.T) {
	t.Parallel()

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

	pin, err := rpcpin.New("http://"+ln.Addr().String(), net.Dialer{})
	require.NoError(t, err)

	a, err := pin.DialContext(t.Context(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	b, err := pin.DialContext(t.Context(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	require.NoError(t, b.Close())

	require.Equal(t, 1, pin.CloseConns(), "the open one; the closed one was forgotten")
	_, err = a.Write([]byte("x"))
	require.ErrorIs(t, err, net.ErrClosed)
	require.Equal(t, 0, pin.CloseConns())
}
