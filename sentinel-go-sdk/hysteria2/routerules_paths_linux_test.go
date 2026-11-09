//go:build linux

package hysteria2

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// stopFailsProcess stands in for a hysteria2 that was already SIGKILL'd (the protocol-kill path):
// Terminate's Stop call on it reports failure.
type stopFailsProcess struct{}

func (stopFailsProcess) Done() <-chan struct{}      { return nil }
func (stopFailsProcess) Err() error                 { return nil }
func (stopFailsProcess) PID() int                   { return os.Getpid() }
func (stopFailsProcess) Stop(context.Context) error { return errors.New("process already gone") }
func (stopFailsProcess) Wait(context.Context) error { return nil }

// stubShowRules replaces ipRule with a stub that records every "rule show" and returns no rule of
// ours, so removeRouteRules deletes nothing but a call proves it ran. It returns the recorded shows.
func stubShowRules(t *testing.T) *[]string {
	t.Helper()

	var shown []string

	orig := ipRule
	t.Cleanup(func() { ipRule = orig })

	ipRule = func(_ context.Context, args ...string) ([]byte, error) {
		if len(args) >= 3 && args[2] == "show" {
			shown = append(shown, strings.Join(args, " "))

			return []byte("0:\tfrom all lookup local\n32766:\tfrom all lookup main\n"), nil
		}

		return nil, nil
	}

	return &shown
}

// readyResp builds a peer response this client accepts, so Start reaches Launch.
func readyResp(t *testing.T, c *Client) service.PeerResponse {
	t.Helper()

	req, err := c.PeerRequest(t.Context())
	require.NoError(t, err)

	var sent PeerRequest
	require.NoError(t, req.Unpack(&sent))

	resp, err := service.PackResponse(Name, service.PeerID(sent.UUID), PeerResponse{
		Auth:   sent.UUID,
		Port:   443,
		TLSPin: strings.TrimSuffix(strings.Repeat("AB:", 32), ":"),
	})
	require.NoError(t, err)

	return resp
}

// Stop must remove sing-tun's policy rules even when Terminate fails because hysteria2 was already
// killed (the protocol-kill path). Before the fix Stop returned the Terminate error first and never
// reached removeRouteRules, so the rules outlived the session.
func TestClient_Stop_RemovesRouteRulesWhenTerminateFails(t *testing.T) { //nolint:paralleltest // swaps ipRule
	shown := stubShowRules(t)

	p := service.Placement{Dir: t.TempDir(), Interface: "lo", ToolDir: t.TempDir()}
	start := func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
		return stopFailsProcess{}, nil
	}

	c := newClient(p, zerolog.Nop(), service.WithProcessStarter(start))
	require.NoError(t, c.Start(t.Context(), "203.0.113.1", readyResp(t, c)))

	require.Error(t, c.Stop(t.Context()), "Terminate fails, so Stop surfaces the error")
	require.NotEmpty(t, *shown, "Stop must remove sing-tun's rules even when Terminate fails")
}

// A cancelled or failed start must remove any sing-tun rules hysteria2 left before dying, so a
// retried connect starts clean.
func TestClient_Start_FailureRemovesRouteRules(t *testing.T) { //nolint:paralleltest // swaps ipRule
	shown := stubShowRules(t)

	p := service.Placement{Dir: t.TempDir(), Interface: "lo", ToolDir: t.TempDir()}
	start := func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
		return nil, errors.New("launch refused")
	}

	c := newClient(p, zerolog.Nop(), service.WithProcessStarter(start))

	require.Error(t, c.Start(t.Context(), "203.0.113.1", readyResp(t, c)))
	require.NotEmpty(t, *shown, "a failed start must remove sing-tun's leftover rules")
}
