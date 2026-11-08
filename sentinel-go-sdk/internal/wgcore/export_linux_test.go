package wgcore

import (
	"net/netip"

	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

func NewServerForTest[E service.Validator](
	v Variant,
	cfg ServerConfig[E],
	p service.Placement,
	log zerolog.Logger,
	r runner,
	setup setupFunc,
) (*Server[E], error) {
	return newServer(v, cfg, p, log, r, setup)
}

func SetServerStarted[E service.Validator](s *Server[E]) {
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
}

func SetFlushConntrackForTest[E service.Validator](s *Server[E], fn func([]netip.Addr) (uint, error)) {
	s.flushConntrack = fn
}
