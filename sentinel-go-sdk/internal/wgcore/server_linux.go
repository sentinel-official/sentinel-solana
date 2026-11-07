package wgcore

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"

	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

const (
	serverTemplate = "templates/server.conf.tmpl"
)

var _ service.PeerRestorer = (*Server[service.Validator])(nil)

type Server[E service.Validator] struct {
	cfg       ServerConfig[E]
	conf      string
	egress    service.Egress
	log       zerolog.Logger
	metadata  service.ServiceMetadata
	mu        sync.Mutex
	peers     *service.Peers[[]netip.Prefix]
	placement service.Placement
	pool      *service.Pool
	publicKey string
	runner    runner
	setup     setupFunc
	started   bool
	stop      service.StopOnce
	stopped   chan struct{}
	variant   Variant
}

func NewServer[E service.Validator](
	v Variant,
	cfg ServerConfig[E],
	p service.Placement,
	log zerolog.Logger,
) (service.Server, error) {
	if err := service.ValidateInterfaceName(p.Interface); err != nil {
		return nil, err
	}

	s, err := newServer(v, cfg, p, log, newRunner(v), service.SetupEgress)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func newServer[E service.Validator](
	v Variant,
	cfg ServerConfig[E],
	p service.Placement,
	log zerolog.Logger,
	r runner,
	setup setupFunc,
) (*Server[E], error) {
	key, err := DerivePublicKey(cfg.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("%w: private key: %w", validate.ErrInvalidConfig, err)
	}

	pool, err := service.NewPool(cfg.Addresses...)
	if err != nil {
		return nil, fmt.Errorf("creating address pool: %w", err)
	}

	return &Server[E]{
		cfg:       cfg,
		conf:      p.ToolPath(".conf"),
		log:       service.ScopeLogger(log, v.Name),
		peers:     service.NewPeers[[]netip.Prefix](),
		placement: p,
		pool:      pool,
		publicKey: key,
		runner:    r,
		setup:     setup,
		stopped:   make(chan struct{}),
		variant:   v,
	}, nil
}

func (s *Server[E]) AddPeer(ctx context.Context, req service.PeerRequest) (service.PeerResponse, error) {
	p, err := service.UnpackRequest[PeerRequest](req)
	if err != nil {
		return service.PeerResponse{}, err
	}

	id := service.PeerID(p.PublicKey)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running() {
		return service.PeerResponse{}, fmt.Errorf("adding peer %s: %w", id, service.ErrServiceNotStarted)
	}

	if s.peers.Has(id) {
		return service.PeerResponse{}, fmt.Errorf("%w: %s", service.ErrPeerExists, id)
	}

	prefixes, err := s.pool.Acquire()
	if err != nil {
		return service.PeerResponse{}, fmt.Errorf("acquiring address: %w", err)
	}

	if err := s.runner.SetPeer(ctx, s.placement.Interface, p.PublicKey, prefixes); err != nil {
		s.pool.Release(prefixes)

		return service.PeerResponse{}, fmt.Errorf("adding peer %s: %w", id, err)
	}

	resp := PeerResponse[E]{
		Addresses:  prefixStrings(prefixes),
		Extensions: s.cfg.Extensions,
		MTU:        serverMTU(s.cfg.MTU, s.cfg.Extensions),
		Port:       s.cfg.ListenPort,
		PublicKey:  s.publicKey,
	}

	packed, err := service.PackResponse(s.variant.Name, id, resp)
	if err != nil {
		return service.PeerResponse{}, s.rollbackPeer(ctx, id, prefixes, err)
	}

	if err := s.peers.Add(id, prefixes); err != nil {
		return service.PeerResponse{}, s.rollbackPeer(ctx, id, prefixes, err)
	}

	s.log.Debug().Str("peer", string(id)).Msg("peer added")

	return packed, nil
}

func (s *Server[E]) Metadata() service.ServiceMetadata {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.metadata
}

func (s *Server[E]) Name() service.Name {
	return s.variant.Name
}

func (s *Server[E]) PeerStats(ctx context.Context, id service.PeerID) (service.PeerStats, error) {
	s.mu.Lock()
	started, known := s.running(), s.peers.Has(id)
	s.mu.Unlock()

	if !started {
		return service.PeerStats{}, fmt.Errorf("querying stats for %s: %w", id, service.ErrServiceNotStarted)
	}

	if !known {
		return service.PeerStats{}, fmt.Errorf("%w: %s", service.ErrPeerNotFound, id)
	}

	transfer, err := s.runner.Transfer(ctx, s.placement.Interface)
	if err != nil {
		return service.PeerStats{}, fmt.Errorf("querying stats: %w", err)
	}

	return transfer[string(id)], nil
}

func (s *Server[E]) RemovePeer(ctx context.Context, id service.PeerID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running() {
		return fmt.Errorf("removing peer %s: %w", id, service.ErrServiceNotStarted)
	}

	prefixes, err := s.peers.Get(id)
	if err != nil {
		return err
	}

	if err := s.runner.RemovePeer(ctx, s.placement.Interface, string(id)); err != nil {
		return fmt.Errorf("removing peer %s: %w", id, err)
	}

	if err := s.peers.Remove(id); err != nil {
		return err
	}

	s.pool.Release(prefixes)
	s.log.Debug().Str("peer", string(id)).Msg("peer removed")

	return nil
}

func (s *Server[E]) RestorePeer(ctx context.Context, req service.PeerRequest, resp service.PeerResponse) error {
	p, err := service.UnpackRequest[PeerRequest](req)
	if err != nil {
		return err
	}

	if resp.ID != service.PeerID(p.PublicKey) {
		return fmt.Errorf("%w: response peer ID %s", service.ErrInvalidPeer, resp.ID)
	}

	var typed PeerResponse[E]
	if err := resp.Unpack(&typed); err != nil {
		return fmt.Errorf("unpacking peer response: %w", err)
	}

	prefixes, err := service.ParsePrefixes(typed.Addresses)
	if err != nil {
		return err
	}

	if len(prefixes) != len(s.cfg.Addresses) {
		return fmt.Errorf("%w: expected %d addresses, got %d", service.ErrInvalidPeer, len(s.cfg.Addresses), len(prefixes))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.peers.Has(resp.ID) {
		return fmt.Errorf("%w: %s", service.ErrPeerExists, resp.ID)
	}

	reserved := make([]netip.Prefix, 0, len(prefixes))
	defer func() { s.pool.Release(reserved) }()

	matched := make([]bool, len(s.cfg.Addresses))

	for _, prefix := range prefixes {
		index := -1

		for i, family := range s.cfg.Addresses {
			if !matched[i] && family.Contains(prefix.Addr()) {
				index = i

				break
			}
		}

		if prefix.Bits() != prefix.Addr().BitLen() || index < 0 {
			return fmt.Errorf("%w: address %s", service.ErrInvalidPeer, prefix)
		}

		if err := s.pool.Reserve(prefix.Addr()); err != nil {
			return fmt.Errorf("reserving address %s: %w", prefix, err)
		}

		matched[index] = true

		reserved = append(reserved, prefix)
	}

	if err := s.runner.SetPeer(ctx, s.placement.Interface, p.PublicKey, prefixes); err != nil {
		return fmt.Errorf("restoring peer %s: %w", resp.ID, err)
	}

	if err := s.peers.Add(resp.ID, prefixes); err != nil {
		return err
	}

	reserved = nil

	return nil
}

func (s *Server[E]) Start(ctx context.Context) error {
	data := serverTemplateData[E]{
		Addresses:  prefixStrings(s.cfg.Addresses),
		Extensions: s.cfg.Extensions,
		ListenPort: s.cfg.ListenPort,
		MTU:        serverMTU(s.cfg.MTU, s.cfg.Extensions),
		PrivateKey: s.cfg.PrivateKey,
	}

	if err := service.WriteTemplate(s.variant.Templates, serverTemplate, s.conf, data, "#"); err != nil {
		return err
	}

	succeeded := false

	defer func() {
		if !succeeded {
			_ = service.RemoveOwned(s.placement.ToolDir, s.placement.Interface)
		}
	}()

	m := Metadata[E]{
		Extensions: s.cfg.Extensions,
		Port:       s.cfg.ListenPort,
		PublicKey:  s.publicKey,
	}

	md, err := service.PackMetadata(s.variant.Name, m)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.metadata = md
	s.mu.Unlock()

	_ = s.runner.Down(ctx, s.conf)

	if _, err := s.runner.Up(ctx, s.conf); err != nil {
		downAfterFailedStart(ctx, s.runner, s.conf)

		return fmt.Errorf("starting %s: %w", s.placement.Interface, err)
	}

	probe := func(ctx context.Context) error {
		if _, err := s.runner.Transfer(ctx, s.placement.Interface); err != nil {
			return fmt.Errorf("querying stats: %w", err)
		}

		return nil
	}

	if err := service.PollReady(ctx, probe); err != nil {
		downAfterFailedStart(ctx, s.runner, s.conf)

		return fmt.Errorf("starting %s: %w", s.placement.Interface, err)
	}

	egress, err := s.setup(ctx, s.placement.Table, s.placement.Interface, s.cfg.EgressInterface, s.cfg.Addresses)
	if err != nil {
		downAfterFailedStart(ctx, s.runner, s.conf)

		return fmt.Errorf("setting up egress: %w", err)
	}

	s.egress = egress
	s.started = true
	succeeded = true

	s.log.Info().Str("iface", s.placement.Interface).Msg("service started")

	return nil
}

func (s *Server[E]) Stop(ctx context.Context) error {
	return s.stop.Do(func() error {
		close(s.stopped)

		if !s.started {
			return nil
		}

		var errs []error

		if s.egress != nil {
			if err := s.egress.Teardown(ctx); err != nil {
				errs = append(errs, fmt.Errorf("tearing down egress: %w", err))
			}
		}

		if err := s.runner.Down(ctx, s.conf); err != nil {
			errs = append(errs, fmt.Errorf("stopping %s: %w", s.placement.Interface, err))
		} else if err := service.RemoveOwned(s.placement.ToolDir, s.placement.Interface); err != nil {
			errs = append(errs, err)
		}

		err := errors.Join(errs...)
		if err == nil {
			s.log.Info().Str("iface", s.placement.Interface).Msg("service stopped")
		}

		return err
	})
}

func (s *Server[E]) Wait(ctx context.Context) error {
	if !s.started {
		return fmt.Errorf("waiting for %s: %w", s.placement.Interface, service.ErrServiceNotStarted)
	}

	return watchInterface(ctx, s.stopped, s.runner, s.placement.Interface)
}

func (s *Server[E]) rollbackPeer(ctx context.Context, id service.PeerID, prefixes []netip.Prefix, cause error) error {
	err := s.runner.RemovePeer(ctx, s.placement.Interface, string(id))
	s.pool.Release(prefixes)

	return errors.Join(cause, err)
}

func (s *Server[E]) running() bool {
	return s.started && !isClosed(s.stopped)
}

type serverTemplateData[E service.Validator] struct {
	Addresses  []string
	Extensions E
	ListenPort uint16
	MTU        uint16
	PrivateKey string
}

type setupFunc func(ctx context.Context, table, iface, outIface string, prefixes []netip.Prefix) (service.Egress, error)

func prefixStrings(prefixes []netip.Prefix) []string {
	out := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		out = append(out, p.String())
	}

	return out
}
