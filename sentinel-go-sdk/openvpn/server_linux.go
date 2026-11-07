package openvpn

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/vishvananda/netlink"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

const (
	passwordBytes          = 32
	removePeerPollInterval = 250 * time.Millisecond
	removePeerTimeout      = 5 * time.Second
	serverTemplate         = "templates/server.conf.tmpl"
	serverToolDir          = "/etc/openvpn/server/" + instance.ProductName
)

var (
	errInvalidManagementStatus = errors.New("invalid management status")
	errPeerNotRemoved          = errors.New("peer not removed")
)

type Server struct {
	cfg           ServerConfig
	conf          string
	egress        service.Egress
	log           zerolog.Logger
	lookupLink    func(string) (netlink.Link, error)
	metadata      service.ServiceMetadata
	mgmt          *management
	mu            sync.Mutex
	peers         *service.Peers[struct{}]
	placement     service.Placement
	process       *service.ProcessManager
	removeTimeout time.Duration
	secret        []byte
	setup         setupFunc
}

func NewServer(cfg ServerConfig, p service.Placement, log zerolog.Logger) (service.Server, error) {
	mgmt := newManagement(map[string]string{})

	s, err := newServer(cfg, p, log, mgmt, service.SetupEgress)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func newServer(
	cfg ServerConfig,
	p service.Placement,
	log zerolog.Logger,
	mgmt *management,
	setup setupFunc,
	opts ...service.ProcessManagerOption,
) (*Server, error) {
	log = service.ScopeLogger(log, Name)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if err := service.ValidateInterfaceName(p.Interface); err != nil {
		return nil, err
	}

	secret, err := loadOrCreatePeerSecret(p.Dir)
	if err != nil {
		return nil, fmt.Errorf("loading peer secret: %w", err)
	}

	return &Server{
		cfg:           cfg,
		conf:          serverConf(p),
		log:           log,
		lookupLink:    netlink.LinkByName,
		mgmt:          mgmt,
		peers:         service.NewPeers[struct{}](),
		placement:     p,
		process:       service.NewProcessManager(binary, p.Dir, log, opts...),
		removeTimeout: removePeerTimeout,
		secret:        secret,
		setup:         setup,
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

	var md Metadata
	if err := s.metadata.Unpack(&md); err != nil {
		return service.PeerResponse{}, fmt.Errorf("unpacking service metadata: %w", err)
	}

	password := s.peerPassword(id)

	s.mgmt.setPassword(id, password)

	resp := PeerResponse{
		CA:       md.CA,
		Password: password,
		Port:     s.cfg.ListenPort,
		Protocol: s.cfg.Protocol,
		TLSCrypt: md.TLSCrypt,
		Username: p.UUID,
	}

	packed, err := service.PackResponse(Name, id, resp)
	if err != nil {
		s.mgmt.removeUser(id)

		return service.PeerResponse{}, err
	}

	if err := s.peers.Add(id, struct{}{}); err != nil {
		s.mgmt.removeUser(id)

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

func (s *Server) PeerStats(_ context.Context, id service.PeerID) (service.PeerStats, error) {
	s.mu.Lock()
	started, known := s.process.Started(), s.peers.Has(id)
	s.mu.Unlock()

	if !started {
		return service.PeerStats{}, fmt.Errorf("querying stats for %s: %w", id, service.ErrServiceNotStarted)
	}

	if !known {
		return service.PeerStats{}, fmt.Errorf("%w: %s", service.ErrPeerNotFound, id)
	}

	out, err := s.mgmt.exec("status 3")
	if err != nil {
		return service.PeerStats{}, fmt.Errorf("querying stats: %w", err)
	}

	return parseStatus(out)[string(id)], nil
}

func (s *Server) RemovePeer(ctx context.Context, id service.PeerID) error {
	s.mu.Lock()

	if !s.process.Started() {
		s.mu.Unlock()

		return fmt.Errorf("removing peer %s: %w", id, service.ErrServiceNotStarted)
	}

	if !s.peers.Has(id) {
		s.mu.Unlock()

		return fmt.Errorf("%w: %s", service.ErrPeerNotFound, id)
	}

	cids := s.mgmt.clientIDs(id)
	s.mgmt.removeUser(id)
	s.mu.Unlock()

	// A kill can fail only because the client already left (crash-killed: "ERROR: no such client").
	// The status poll decides: gone means removed. Failing on the kill error kept the peer and the
	// node retried the close forever under closeMu.
	var killErrs []error

	for _, cid := range cids {
		if _, err := s.mgmt.exec("client-kill " + cid + " HALT"); err != nil {
			killErrs = append(killErrs, err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, s.removeTimeout)
	defer cancel()

	goneErr := s.waitPeerGone(ctx, string(id))
	if errors.Is(goneErr, errPeerNotRemoved) {
		if _, killErr := s.mgmt.exec("kill " + string(id)); killErr != nil {
			killErrs = append(killErrs, killErr)
		}

		goneErr = s.waitPeerGone(ctx, string(id))
	}

	if goneErr != nil {
		return fmt.Errorf("removing peer %s: %w", id, errors.Join(append(killErrs, goneErr)...))
	}

	if len(killErrs) > 0 {
		s.log.Debug().Err(errors.Join(killErrs...)).Str("peer", string(id)).Msg("kill failed but the peer is gone")
	}

	s.mu.Lock()
	_ = s.peers.Remove(id)
	s.mu.Unlock()

	s.log.Debug().Str("peer", string(id)).Msg("peer removed")

	return nil
}

func (s *Server) Start(ctx context.Context) error {
	if err := s.process.KillStale(ctx, configFlag, s.conf); err != nil {
		return fmt.Errorf("stopping previous %s: %w", binary, err)
	}

	if err := s.checkInterface(); err != nil {
		return err
	}

	tlsCryptPath := filepath.Join(s.placement.Dir, "tls-crypt.key")

	certPEM, tlsCrypt, err := s.loadCredentials(tlsCryptPath)
	if err != nil {
		return err
	}

	prefixes, err := service.ParsePrefixes(s.cfg.Addresses)
	if err != nil {
		return err
	}

	succeeded := false

	defer func() {
		if !succeeded {
			_ = service.RemoveOwned(s.placement.ToolDir, s.placement.Interface)
		}
	}()

	if err := s.writeConf(tlsCryptPath, prefixes[0]); err != nil {
		return err
	}

	m := Metadata{
		CA:       string(certPEM),
		Port:     s.cfg.ListenPort,
		Protocol: s.cfg.Protocol,
		TLSCrypt: string(tlsCrypt),
	}

	md, err := service.PackMetadata(Name, m)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.metadata = md
	s.mu.Unlock()

	cmd := service.Command{
		Args: []string{configFlag, s.conf},
		Path: binary,
	}

	if err := s.process.Launch(ctx, cmd, s.probeManagement); err != nil {
		return err
	}

	egress, err := s.setup(ctx, s.placement.Table, s.placement.Interface, s.cfg.EgressInterface, prefixes)
	if err != nil {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), service.FailedStartStopTimeout)
		defer cancel()

		return errors.Join(fmt.Errorf("setting up egress: %w", err), s.Stop(stopCtx))
	}

	s.egress = egress
	succeeded = true

	s.log.Info().Str("iface", s.placement.Interface).Str("process", binary).Msg("service started")

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.process.Do(func() error {
		if !s.process.Started() {
			return nil
		}

		var errs []error

		if s.egress != nil {
			if err := s.egress.Teardown(ctx); err != nil {
				errs = append(errs, fmt.Errorf("tearing down egress: %w", err))
			}
		}

		if err := s.process.Terminate(ctx); err != nil {
			errs = append(errs, err)
		} else if err := service.RemoveOwned(s.placement.ToolDir, s.placement.Interface); err != nil {
			errs = append(errs, err)
		}

		err := errors.Join(errs...)
		if err == nil {
			s.log.Info().Str("iface", s.placement.Interface).Str("process", binary).Msg("service stopped")
		}

		return err
	})
}

func (s *Server) Wait(ctx context.Context) error {
	return s.process.Wait(ctx)
}

func (s *Server) checkInterface() error {
	if _, err := s.lookupLink(s.placement.Interface); err != nil {
		if _, ok := errors.AsType[netlink.LinkNotFoundError](err); !ok {
			return fmt.Errorf("checking %s: %w", s.placement.Interface, err)
		}
	} else {
		return fmt.Errorf("interface %s already exists; remove it explicitly before starting", s.placement.Interface)
	}

	return nil
}

func (s *Server) loadCredentials(tlsCryptPath string) (certPEM, tlsCrypt []byte, err error) {
	cert, _, err := api.LoadOrCreateServerCert(s.placement.Dir)
	if err != nil {
		return nil, nil, fmt.Errorf("loading server certificate: %w", err)
	}

	certPEM = api.EncodeCertificatePEM(cert)

	if _, err := os.Stat(tlsCryptPath); err != nil {
		key, err := newTLSCryptKey()
		if err != nil {
			return nil, nil, err
		}

		if err := os.WriteFile(tlsCryptPath, key, 0o600); err != nil {
			return nil, nil, fmt.Errorf("writing tls-crypt key: %w", err)
		}
	}

	tlsCrypt, err = os.ReadFile(tlsCryptPath) //#nosec G304
	if err != nil {
		return nil, nil, fmt.Errorf("reading tls-crypt key: %w", err)
	}

	return certPEM, tlsCrypt, nil
}

func (s *Server) peerPassword(id service.PeerID) string {
	mac := hmac.New(sha256.New, s.secret)
	_, _ = io.WriteString(mac, string(id))

	return hex.EncodeToString(mac.Sum(nil)[:passwordBytes])
}

func (s *Server) probeManagement(ctx context.Context) error {
	addr := service.LoopbackAddr(s.cfg.ManagementPort)
	dialer := net.Dialer{}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dialing %s: %w", addr, err)
	}

	s.mgmt.attach(conn)

	serveMgmt := func() {
		defer func() { _ = conn.Close() }()

		if err := s.mgmt.serve(bufio.NewReader(conn), conn); err != nil {
			s.log.Error().Err(err).Msg("management connection failed")
		}
	}
	go serveMgmt()

	return nil
}

func (s *Server) waitPeerGone(ctx context.Context, user string) error {
	ticker := time.NewTicker(removePeerPollInterval)
	defer ticker.Stop()

	for {
		out, err := s.mgmt.exec("status 3")
		if err != nil {
			return fmt.Errorf("querying peer status: %w", err)
		}

		if !validStatus(out) {
			return errInvalidManagementStatus
		}

		if _, connected := parseStatus(out)[user]; !connected {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s", errPeerNotRemoved, user)
		case <-ticker.C:
		}
	}
}

func (s *Server) writeConf(tlsCryptPath string, prefix netip.Prefix) error {
	network, netmask := networkAndNetmask(prefix)

	data := serverTemplateData{
		CertFile:       s.placement.ToolPath(".tls.crt"),
		Interface:      s.placement.Interface,
		KeyFile:        s.placement.ToolPath(".tls.key"),
		ManagementPort: s.cfg.ManagementPort,
		Netmask:        netmask,
		Network:        network,
		Port:           s.cfg.ListenPort,
		Protocol:       s.cfg.Protocol,
		TLSCryptFile:   s.placement.ToolPath(".tls-crypt.key"),
		TmpDir:         s.placement.ToolDir,
	}

	if err := service.CopyOwned(filepath.Join(s.placement.Dir, "tls.crt"), data.CertFile, "#"); err != nil {
		return err
	}

	if err := service.CopyOwned(filepath.Join(s.placement.Dir, "tls.key"), data.KeyFile, "#"); err != nil {
		return err
	}

	if err := service.CopyOwned(tlsCryptPath, data.TLSCryptFile, "#"); err != nil {
		return err
	}

	return service.WriteTemplate(templates, serverTemplate, s.conf, data, "#")
}

type serverTemplateData struct {
	CertFile       string
	Interface      string
	KeyFile        string
	ManagementPort uint16
	Netmask        string
	Network        string
	Port           uint16
	Protocol       string
	TLSCryptFile   string
	TmpDir         string
}

type setupFunc func(ctx context.Context, table, iface, outIface string, prefixes []netip.Prefix) (service.Egress, error)

func (m *management) clientIDs(id service.PeerID) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]string, 0, len(m.cids[string(id)]))
	for cid := range m.cids[string(id)] {
		out = append(out, cid)
	}

	return out
}

func (m *management) removeUser(id service.PeerID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.credentials, string(id))
	delete(m.cids, string(id))
}

func (m *management) setPassword(id service.PeerID, password string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.credentials[string(id)] = password
}

//nolint:gochecknoinits
func init() {
	driver := service.ServerDriver[ServerConfig]{
		Binaries: []string{binary},
		Bit:      protocolBit,
		Cleanup:  cleanupServer,
		Defaults: DefaultServerConfig,
		New:      NewServer,
		ToolDir:  serverToolDir,
	}
	service.RegisterServer(Name, driver)
}

func cleanupServer(ctx context.Context, p service.Placement) error {
	return service.KillPIDFile(ctx, service.PIDPath(p.Dir, binary), binary, configFlag, serverConf(p))
}

func loadOrCreatePeerSecret(dir string) (secret []byte, result error) {
	path := filepath.Join(dir, "peer-secret.key")

	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0) //#nosec G304
	if err == nil {
		defer func() {
			if err := file.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("closing peer secret: %w", err))
			}
		}()

		info, err := file.Stat()
		if err != nil {
			return nil, fmt.Errorf("statting peer secret: %w", err)
		}

		if info.Mode() != 0o600 {
			return nil, fmt.Errorf("%w: peer secret mode %s", validate.ErrInvalidConfig, info.Mode())
		}

		secret, err = io.ReadAll(io.LimitReader(file, passwordBytes+1))
		if err != nil {
			return nil, fmt.Errorf("reading peer secret: %w", err)
		}

		if len(secret) != passwordBytes {
			return nil, fmt.Errorf("%w: peer secret length %d", validate.ErrInvalidConfig, len(secret))
		}

		return secret, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reading peer secret: %w", err)
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating peer secret directory: %w", err)
	}

	secret = make([]byte, passwordBytes)

	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generating peer secret: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".peer-secret-*")
	if err != nil {
		return nil, fmt.Errorf("creating peer secret: %w", err)
	}
	defer func() {
		if err := os.Remove(tmp.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("removing temporary peer secret: %w", err))
		}
	}()

	if _, err := tmp.Write(secret); err != nil {
		writeErr := fmt.Errorf("writing peer secret: %w", err)
		if closeErr := tmp.Close(); closeErr != nil {
			return nil, errors.Join(writeErr, fmt.Errorf("closing peer secret: %w", closeErr))
		}

		return nil, writeErr
	}

	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("closing peer secret: %w", err)
	}

	if err := os.Link(tmp.Name(), path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return loadOrCreatePeerSecret(dir)
		}

		return nil, fmt.Errorf("installing peer secret: %w", err)
	}

	return secret, nil
}

func networkAndNetmask(p netip.Prefix) (string, string) {
	network := p.Masked().Addr()
	mask := net.CIDRMask(p.Bits(), network.BitLen())

	return network.String(), net.IP(mask).String()
}

func serverConf(p service.Placement) string {
	return p.ToolPath(".conf")
}

func validStatus(out string) bool {
	if !strings.HasSuffix(strings.TrimSpace(out), "\nEND") {
		return false
	}

	var header []string

	for line := range strings.SplitSeq(out, "\n") {
		fields := strings.Split(line, "\t")
		switch fields[0] {
		case "HEADER":
			if len(fields) > 1 && fields[1] == "CLIENT_LIST" {
				header = fields[2:]
				if headerIndex(header, "Username") < 0 ||
					headerIndex(header, "Bytes Received") < 0 ||
					headerIndex(header, "Bytes Sent") < 0 {
					return false
				}
			}
		case "CLIENT_LIST":
			if header == nil || len(fields) != len(header)+1 || fields[headerIndex(header, "Username")+1] == "" {
				return false
			}
		}
	}

	return header != nil
}
