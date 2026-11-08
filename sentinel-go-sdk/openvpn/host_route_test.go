package openvpn_test

import (
	"context"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/openvpn"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// routeTable fakes `ip route` for one main table.
type routeTable struct {
	mu      sync.Mutex
	routes  map[string]string // server address -> route line
	deletes [][]string
}

func (t *routeTable) run(_ context.Context, cmd service.Command) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch {
	case slices.Contains(cmd.Args, "show"):
		target := cmd.Args[slices.Index(cmd.Args, "exact")+1]
		host, _, _ := strings.Cut(target, "/")

		if line, ok := t.routes[host]; ok {
			return []byte(line + "\n"), nil
		}

		return nil, nil
	case slices.Contains(cmd.Args, "del"):
		t.deletes = append(t.deletes, cmd.Args)
		host := cmd.Args[slices.Index(cmd.Args, "del")+1]
		delete(t.routes, host)

		return nil, nil
	}

	return nil, nil
}

func (t *routeTable) set(host, line string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if line == "" {
		delete(t.routes, host)
	} else {
		t.routes[host] = line
	}
}

func (t *routeTable) get(host string) string {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.routes[host]
}

const (
	routeServer = "203.0.113.1"
	routeLine   = "203.0.113.1 via 192.0.2.1 dev eth0"
)

// startWithRoute starts a client whose fake openvpn adds the server host route, as redirect-gateway does.
func startWithRoute(t *testing.T, table *routeTable, p service.Placement, proc service.Process) *openvpn.Client {
	t.Helper()

	listener, _ := startClientManagement(t, "CONNECTED,SUCCESS", nil)
	cfg := openvpn.DefaultClientConfig()
	cfg.ManagementPort = uint16(listener.Addr().(*net.TCPAddr).Port)

	client := openvpn.NewClientForTest(cfg, p, zerolog.Nop(),
		service.WithProcessStarter(func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
			table.set(routeServer, routeLine)

			return proc, nil
		}),
	)
	openvpn.SetHostRouteRunner(client, "linux", table.run)
	require.NoError(t, client.Start(t.Context(), routeServer, peerResponseFixture(t, client)))

	return client
}

// The openvpn process is killed (SIGKILL: it never removes its route), and the next daemon start runs Cleanup.
func TestClient_Cleanup_RemovesTheHostRouteAKilledOpenVPNLeft(t *testing.T) {
	t.Parallel()

	table := &routeTable{routes: map[string]string{}}
	p := service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0", ToolDir: t.TempDir()}
	startWithRoute(t, table, p, failingStopProcess{})
	require.Equal(t, routeLine, table.get(routeServer))

	restarted := openvpn.NewClientForTest(openvpn.DefaultClientConfig(), p, zerolog.Nop())
	openvpn.SetHostRouteRunner(restarted, "linux", table.run)
	require.NoError(t, restarted.Cleanup(t.Context()))

	require.Empty(t, table.get(routeServer), "the host route openvpn added is gone after Cleanup")
}

// killedProcess is an openvpn that was SIGKILLed: it has exited, and it never removed its route.
type killedProcess struct{ done chan struct{} }

func newKilledProcess() killedProcess {
	done := make(chan struct{})
	close(done)

	return killedProcess{done: done}
}

func (p killedProcess) Done() <-chan struct{}    { return p.done }
func (killedProcess) Err() error                 { return nil }
func (killedProcess) PID() int                   { return 4242 }
func (killedProcess) Stop(context.Context) error { return nil }
func (killedProcess) Wait(context.Context) error { return nil }

// The openvpn process is killed during the session, and the client then stops the session in the same daemon.
func TestClient_Stop_RemovesTheHostRouteAKilledOpenVPNLeft(t *testing.T) {
	t.Parallel()

	table := &routeTable{routes: map[string]string{}}
	p := service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0", ToolDir: t.TempDir()}
	client := startWithRoute(t, table, p, newKilledProcess())
	require.Equal(t, routeLine, table.get(routeServer))

	require.NoError(t, client.Stop(t.Context()))

	require.Empty(t, table.get(routeServer), "the host route openvpn added is gone after Stop")
}

func TestClient_Cleanup_KeepsAHostRouteThatChanged(t *testing.T) {
	t.Parallel()

	table := &routeTable{routes: map[string]string{}}
	p := service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0", ToolDir: t.TempDir()}
	startWithRoute(t, table, p, failingStopProcess{})

	other := "203.0.113.1 via 192.0.2.254 dev eth1"
	table.set(routeServer, other) // someone else owns the route now

	restarted := openvpn.NewClientForTest(openvpn.DefaultClientConfig(), p, zerolog.Nop())
	openvpn.SetHostRouteRunner(restarted, "linux", table.run)
	require.NoError(t, restarted.Cleanup(t.Context()))

	require.Equal(t, other, table.get(routeServer))
	require.Empty(t, table.deletes)
}

func TestClient_Start_DoesNotRecordAHostRouteThatWasThereBefore(t *testing.T) {
	t.Parallel()

	table := &routeTable{routes: map[string]string{routeServer: routeLine}} // the user's own route
	p := service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0", ToolDir: t.TempDir()}
	startWithRoute(t, table, p, failingStopProcess{})

	restarted := openvpn.NewClientForTest(openvpn.DefaultClientConfig(), p, zerolog.Nop())
	openvpn.SetHostRouteRunner(restarted, "linux", table.run)
	require.NoError(t, restarted.Cleanup(t.Context()))

	require.Equal(t, routeLine, table.get(routeServer))
	require.Empty(t, table.deletes)
}
