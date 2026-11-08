package hysteria2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const (
	requestTimeout = 10 * time.Second
	serverTemplate = "templates/server.yaml.tmpl"
)

//nolint:gochecknoglobals
var httpClient = &http.Client{Timeout: requestTimeout}

type Server struct {
	authServer *http.Server
	cfg        ServerConfig
	conf       string
	log        zerolog.Logger
	metadata   service.ServiceMetadata
	mu         sync.Mutex
	peers      *service.Peers[struct{}]
	placement  service.Placement
	fwBackend  string
	run        runFunc
	process    *service.ProcessManager
	statsAddr  string
	tlsPin     string
}

func NewServer(cfg ServerConfig, p service.Placement, log zerolog.Logger) (service.Server, error) {
	statsAddr := service.LoopbackAddr(cfg.StatsPort)

	s, err := newServer(cfg, p, log, statsAddr)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func newServer(
	cfg ServerConfig,
	p service.Placement,
	log zerolog.Logger,
	statsAddr string,
	opts ...service.ProcessManagerOption,
) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	log = service.ScopeLogger(log, Name)

	return &Server{
		cfg:       cfg,
		conf:      serverConf(p),
		log:       log,
		peers:     service.NewPeers[struct{}](),
		placement: p,
		process:   service.NewProcessManager(binary, p.Dir, log, opts...),
		run:       defaultRun,
		statsAddr: statsAddr,
	}, nil
}

func (s *Server) AddPeer(_ context.Context, req service.PeerRequest) (service.PeerResponse, error) {
	p, err := service.UnpackRequest[PeerRequest](req)
	if err != nil {
		return service.PeerResponse{}, err
	}

	id := service.PeerID(p.UUID)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.process.Started() {
		return service.PeerResponse{}, fmt.Errorf("adding peer %s: %w", id, service.ErrServiceNotStarted)
	}

	if s.peers.Has(id) {
		return service.PeerResponse{}, fmt.Errorf("%w: %s", service.ErrPeerExists, id)
	}

	resp := PeerResponse{
		Auth:         string(id),
		ObfsPassword: s.cfg.ObfsPassword,
		Port:         s.cfg.ListenPort,
		TLSPin:       s.tlsPin,
	}

	packed, err := service.PackResponse(Name, id, resp)
	if err != nil {
		return service.PeerResponse{}, err
	}

	if err := s.peers.Add(id, struct{}{}); err != nil {
		return service.PeerResponse{}, err
	}

	s.log.Debug().Str("peer", string(id)).Msg("peer added")

	return packed, nil
}

func (s *Server) Metadata() service.ServiceMetadata {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.metadata
}

func (s *Server) Name() service.Name {
	return Name
}

func (s *Server) PeerStats(ctx context.Context, id service.PeerID) (service.PeerStats, error) {
	s.mu.Lock()
	started, known := s.process.Started(), s.peers.Has(id)
	s.mu.Unlock()

	if !started {
		return service.PeerStats{}, fmt.Errorf("querying stats for %s: %w", id, service.ErrServiceNotStarted)
	}

	if !known {
		return service.PeerStats{}, fmt.Errorf("%w: %s", service.ErrPeerNotFound, id)
	}

	entries, err := fetchTraffic(ctx, s.statsAddr, s.cfg.StatsSecret)
	if err != nil {
		return service.PeerStats{}, fmt.Errorf("querying stats: %w", err)
	}

	entry := entries[string(id)]

	return service.PeerStats{
		RxBytes: entry.Tx,
		TxBytes: entry.Rx,
	}, nil
}

func (s *Server) RemovePeer(ctx context.Context, id service.PeerID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.process.Started() {
		return fmt.Errorf("removing peer %s: %w", id, service.ErrServiceNotStarted)
	}

	if err := s.peers.Remove(id); err != nil {
		return err
	}

	if err := kick(ctx, s.statsAddr, s.cfg.StatsSecret, string(id)); err != nil {
		errs := []error{err}

		if addErr := s.peers.Add(id, struct{}{}); addErr != nil {
			errs = append(errs, addErr)
		}

		return fmt.Errorf("removing peer %s: %w", id, errors.Join(errs...))
	}

	s.log.Debug().Str("peer", string(id)).Msg("peer removed")

	return nil
}

func (s *Server) Start(ctx context.Context) error {
	if err := s.process.KillStale(ctx, configFlag, s.conf); err != nil {
		return fmt.Errorf("stopping previous %s: %w", binary, err)
	}

	_, pin, err := api.LoadOrCreateServerCert(s.placement.Dir)
	if err != nil {
		return fmt.Errorf("loading server certificate: %w", err)
	}

	s.tlsPin = api.FormatColonFingerprint(pin)

	certFile, keyFile := s.placement.ToolPath(".tls.crt"), s.placement.ToolPath(".tls.key")

	if err := service.CopyOwned(filepath.Join(s.placement.Dir, "tls.crt"), certFile, "#"); err != nil {
		return err
	}

	succeeded := false

	defer func() {
		if !succeeded {
			_ = service.RemoveOwned(s.placement.ToolDir, s.placement.Interface)
		}
	}()

	if err := service.CopyOwned(filepath.Join(s.placement.Dir, "tls.key"), keyFile, "#"); err != nil {
		return err
	}

	data := serverTemplateData{
		AuthPort:      s.cfg.AuthPort,
		CertFile:      certFile,
		KeyFile:       keyFile,
		MasqueradeURL: s.cfg.MasqueradeURL,
		ObfsPassword:  s.cfg.ObfsPassword,
		Port:          s.cfg.ListenPort,
		StatsPort:     s.cfg.StatsPort,
		StatsSecret:   s.cfg.StatsSecret,
	}

	if err := service.WriteTemplate(templates, serverTemplate, s.conf, data, "#"); err != nil {
		return err
	}

	m := Metadata{
		ObfsPassword: s.cfg.ObfsPassword,
		Port:         s.cfg.ListenPort,
		TLSPin:       s.tlsPin,
	}

	md, err := service.PackMetadata(Name, m)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.metadata = md
	s.mu.Unlock()

	if err := s.listenAuthServer(ctx); err != nil {
		return err
	}

	cmd := service.Command{
		Args: []string{"server", configFlag, s.conf},
		Path: binary,
	}

	probe := func(ctx context.Context) error {
		_, err := fetchTraffic(ctx, s.statsAddr, s.cfg.StatsSecret)

		return err
	}

	if err := s.process.Launch(ctx, cmd, probe); err != nil {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), service.FailedStartStopTimeout)
		defer cancel()

		if shutdownErr := s.authServer.Shutdown(stopCtx); shutdownErr != nil {
			err = errors.Join(err, fmt.Errorf("stopping auth server: %w", shutdownErr))
		}

		return err
	}

	// Open the listen port in the host firewall. Best-effort: a firewall we cannot drive must not
	// stop the node from serving. First clean up any opening a previous (possibly crashed) run left.
	// Held under s.mu so a concurrent Stop cannot read s.fwBackend between the open and the assign.
	s.mu.Lock()
	if stale, ok := readFirewallMarker(s.placement.Dir, s.log); ok {
		if err := closeInboundUDP(ctx, s.run, stale.Backend, stale.Port, s.log); err != nil {
			s.log.Warn().Err(err).Msg("cleaning a firewall opening left by a previous run")
		}

		removeFirewallMarker(s.placement.Dir, s.log)
	}

	backend, fwErr := openInboundUDP(ctx, s.run, s.cfg.ListenPort, s.log)
	if fwErr != nil {
		s.log.Warn().Err(fwErr).Msg("opening inbound udp in the host firewall; serving anyway — open it yourself if clients cannot connect")

		backend = fwNone
	}

	s.fwBackend = backend
	if backend != fwNone {
		writeFirewallMarker(s.placement.Dir, firewallMarker{Backend: backend, Port: s.cfg.ListenPort}, s.log)
	}
	s.mu.Unlock()

	succeeded = true

	s.log.Info().Str("process", binary).Msg("service started")

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.process.Do(func() error {
		if !s.process.Started() {
			return nil
		}

		var errs []error

		if err := s.process.Terminate(ctx); err != nil {
			errs = append(errs, err)
		} else if err := service.RemoveOwned(s.placement.ToolDir, s.placement.Interface); err != nil {
			errs = append(errs, err)
		}

		s.mu.Lock()
		if s.fwBackend != "" {
			if err := closeInboundUDP(ctx, s.run, s.fwBackend, s.cfg.ListenPort, s.log); err != nil {
				errs = append(errs, err)
			} else {
				s.fwBackend = ""

				removeFirewallMarker(s.placement.Dir, s.log)
			}
		}
		s.mu.Unlock()

		if err := s.authServer.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stopping auth server: %w", err))
		}

		err := errors.Join(errs...)
		if err == nil {
			s.log.Info().Str("process", binary).Msg("service stopped")
		}

		return err
	})
}

func (s *Server) Wait(ctx context.Context) error {
	return s.process.Wait(ctx)
}

func (s *Server) listenAuthServer(ctx context.Context) error {
	srv := &http.Server{
		Addr:              service.LoopbackAddr(s.cfg.AuthPort),
		Handler:           authHandler(s.peers),
		ReadHeaderTimeout: requestTimeout,
	}

	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("starting auth server: %w", err)
	}

	go s.serveAuthServer(srv, listener)

	s.authServer = srv

	return nil
}

func (s *Server) serveAuthServer(srv *http.Server, listener net.Listener) {
	if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.Error().Err(err).Msg("auth server failed")
	}
}

type authRequest struct {
	Addr string `json:"addr"`
	Auth string `json:"auth"`
	Tx   uint64 `json:"tx"`
}

type authResponse struct {
	ID string `json:"id"`
	OK bool   `json:"ok"`
}

type serverTemplateData struct {
	AuthPort      uint16
	CertFile      string
	KeyFile       string
	MasqueradeURL string
	ObfsPassword  string
	Port          uint16
	StatsPort     uint16
	StatsSecret   string
}

type trafficEntry struct {
	Rx uint64 `json:"rx"`
	Tx uint64 `json:"tx"`
}

//nolint:gochecknoinits
func init() {
	driver := service.ServerDriver[ServerConfig]{
		Binaries: []string{binary},
		Bit:      protocolBit,
		Cleanup:  cleanupServer,
		Defaults: DefaultServerConfig,
		New:      NewServer,
		ToolDir:  toolDir,
	}
	service.RegisterServer(Name, driver)
}

func authHandler(peers *service.Peers[struct{}]) http.Handler {
	handle := func(w http.ResponseWriter, r *http.Request) {
		var req authRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		resp := authResponse{}
		if peers.Has(service.PeerID(req.Auth)) {
			resp = authResponse{
				ID: req.Auth,
				OK: true,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp) //nolint:errchkjson
	}

	return http.HandlerFunc(handle)
}

func cleanupServer(ctx context.Context, p service.Placement) error {
	return service.KillPIDFile(ctx, service.PIDPath(p.Dir, binary), binary, configFlag, serverConf(p))
}

func fetchTraffic(ctx context.Context, addr, secret string) (map[string]trafficEntry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/traffic", nil)
	if err != nil {
		return nil, fmt.Errorf("building traffic request: %w", err)
	}

	req.Header.Set("Authorization", secret)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending traffic request: %w", err)
	}

	closeBody := func() { _ = resp.Body.Close() }
	defer closeBody()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("traffic request: unexpected status %s", resp.Status)
	}

	out := map[string]trafficEntry{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decoding traffic response: %w", err)
	}

	return out, nil
}

func kick(ctx context.Context, addr, secret, id string) error {
	ids := []string{id}

	body, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("encoding kick request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/kick", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building kick request: %w", err)
	}

	req.Header.Set("Authorization", secret)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending kick request: %w", err)
	}

	closeBody := func() { _ = resp.Body.Close() }
	defer closeBody()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kick request: unexpected status %s", resp.Status)
	}

	return nil
}

func serverConf(p service.Placement) string {
	return p.ToolPath(".yaml")
}
