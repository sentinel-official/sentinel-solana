package hysteria2

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

type TrafficEntry = trafficEntry

func AuthHandler(peers *service.Peers[struct{}]) http.Handler { return authHandler(peers) }

func FetchTraffic(ctx context.Context, addr, secret string) (map[string]trafficEntry, error) {
	return fetchTraffic(ctx, addr, secret)
}

func Kick(ctx context.Context, addr, secret, id string) error { return kick(ctx, addr, secret, id) }

func NewServerForTest(
	cfg ServerConfig,
	p service.Placement,
	log zerolog.Logger,
	statsAddr string,
	opts ...service.ProcessManagerOption,
) (*Server, error) {
	s, err := newServer(cfg, p, log, statsAddr, opts...)
	if err != nil {
		return nil, err
	}

	s.run = stubNoFirewall // tests don't touch the host firewall unless they call SetServerRun

	return s, nil
}

// stubNoFirewall makes every firewall probe fail, so openInboundUDP takes the fwNone path (no host I/O).
func stubNoFirewall(context.Context, string, ...string) ([]byte, error) {
	return nil, errors.New("no firewall in test")
}

// SetServerRun overrides the firewall command runner so a test can simulate a backend and record calls.
func SetServerRun(s *Server, run runFunc) { s.run = run }

func ServerServeAuthServer(s *Server, srv *http.Server, listener net.Listener) {
	s.serveAuthServer(srv, listener)
}

func ServerTLSPin(s *Server) string { return s.tlsPin }

func OpenInboundUDPForTest(ctx context.Context, run func(context.Context, string, ...string) ([]byte, error), port uint16, log zerolog.Logger) (string, error) {
	return openInboundUDP(ctx, run, port, log)
}

func CloseInboundUDPForTest(ctx context.Context, run func(context.Context, string, ...string) ([]byte, error), backend string, port uint16, log zerolog.Logger) error {
	return closeInboundUDP(ctx, run, backend, port, log)
}
