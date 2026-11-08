//go:build linux

package hysteria2_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/hysteria2"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// newFirewallTestServer builds a Server whose process start is faked and whose stats endpoint
// returns {}, so Start/Stop run end to end without a real daemon. The caller sets the firewall
// runner with hysteria2.SetServerRun.
func newFirewallTestServer(t *testing.T, p service.Placement) *hysteria2.Server {
	t.Helper()

	statsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(statsSrv.Close)

	start := func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
		return fakeProcess{}, nil
	}

	s, err := hysteria2.NewServerForTest(
		hysteria2.DefaultServerConfig(), p, zerolog.Nop(), statsSrv.Listener.Addr().String(),
		service.WithProcessStarter(start),
	)
	require.NoError(t, err)

	return s
}

// A host firewall the node cannot drive (D-Bus/polkit refusal, permission) must not stop the node
// from serving: Start logs and continues, and since nothing was opened Stop closes nothing.
func TestServer_Start_FirewallError_Continues(t *testing.T) {
	t.Parallel()

	s := newFirewallTestServer(t, serverPlacementFixture(t))

	var calls []string
	hysteria2.SetServerRun(s, func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		if name == "firewall-cmd" && len(args) == 1 && args[0] == "--state" {
			return []byte("running\n"), nil
		}
		if name == "firewall-cmd" && len(args) >= 1 && strings.HasPrefix(args[0], "--add-port=") {
			return []byte("ERROR: access denied"), errors.New("firewall command failed")
		}

		return nil, nil
	})

	require.NoError(t, s.Start(t.Context()), "a firewall open failure must not fail Start")
	require.NoError(t, s.Stop(context.Background()))
	require.False(t, fwHasCallPrefix(calls, "firewall-cmd --remove-port="),
		"nothing was opened, so Stop must not close anything, calls=%v", calls)
}

// A leftover marker from a previous run that crashed before Stop is cleaned up on the next Start,
// so an opening (and, for ufw, a reboot-persistent rule) is never orphaned.
func TestServer_Start_CleansStaleFirewallMarker(t *testing.T) {
	t.Parallel()

	p := serverPlacementFixture(t)
	require.NoError(t, os.WriteFile(
		filepath.Join(p.Dir, "hy2-firewall.json"),
		[]byte(`{"backend":"firewalld","port":9999}`), 0o600))

	s := newFirewallTestServer(t, p)

	var calls []string
	hysteria2.SetServerRun(s, func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		if name == "firewall-cmd" && len(args) == 1 && args[0] == "--state" {
			return []byte("running\n"), nil
		}

		return nil, nil
	})

	require.NoError(t, s.Start(t.Context()))
	require.True(t, fwHasCallPrefix(calls, "firewall-cmd --remove-port=9999/udp"),
		"Start must close the stale opening from the marker, calls=%v", calls)
	require.NoError(t, s.Stop(context.Background()))
}

// A corrupt marker is ignored (and logged): Start does not try to close a bogus opening and still
// opens the real port.
func TestServer_Start_IgnoresCorruptMarker(t *testing.T) {
	t.Parallel()

	p := serverPlacementFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(p.Dir, "hy2-firewall.json"), []byte("not json"), 0o600))

	s := newFirewallTestServer(t, p)

	var calls []string
	hysteria2.SetServerRun(s, func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		if name == "firewall-cmd" && len(args) == 1 && args[0] == "--state" {
			return []byte("running\n"), nil
		}

		return nil, nil
	})

	require.NoError(t, s.Start(t.Context()))
	require.False(t, fwHasCallPrefix(calls, "firewall-cmd --remove-port="),
		"a corrupt marker must not trigger a stale close, calls=%v", calls)
	require.True(t, fwHasCallPrefix(calls, "firewall-cmd --add-port="),
		"Start must still open the real port, calls=%v", calls)
	require.NoError(t, s.Stop(context.Background()))
}
