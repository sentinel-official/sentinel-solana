package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/bits"
	"net"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

const (
	SeverityError   Severity = "error"
	SeverityOK      Severity = "ok"
	SeverityWarning Severity = "warning"
)

const maxHostnameBytes = 253

var (
	ErrInvalidHostname      = errors.New("invalid hostname")
	ErrInvalidName          = errors.New("invalid service name")
	ErrInvalidPeer          = errors.New("invalid peer")
	ErrPeerExists           = errors.New("peer already exists")
	ErrPeerNotFound         = errors.New("peer not found")
	ErrServiceNotStarted    = errors.New("not started")
	ErrUnexpectedConfigType = errors.New("unexpected config type")
	ErrUnknownName          = errors.New("unknown service name")
)

//nolint:gochecknoglobals
var (
	clients      = map[Name]entry[Client]{}
	mu           sync.RWMutex
	protocolBits = map[Name]uint32{}
	servers      = map[Name]entry[Server]{}
)

var (
	labelPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)
	namePattern  = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)
)

type Client interface {
	Interface() string
	Name() Name
	PeerRequest(ctx context.Context) (PeerRequest, error)
	ProxyEndpoint() string
	Start(ctx context.Context, host string, resp PeerResponse) error
	Stats(ctx context.Context) (PeerStats, error)
	Stop(ctx context.Context) error
	Wait(ctx context.Context) error
}

func NewClient(name Name, cfg any, p Placement, log zerolog.Logger) (Client, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}

	mu.RLock()
	defer mu.RUnlock()

	entry, err := lookup(clients, name)
	if err != nil {
		return nil, err
	}

	return entry.construct(cfg, p, log)
}

type ClientCleaner interface {
	Cleanup(ctx context.Context) error
}

type ClientDriver[C any] struct {
	Binaries   []string
	Bit        uint32
	Defaults   func() C
	LookupDirs func() []string
	Metadata   func() Validator
	New        func(cfg C, p Placement, log zerolog.Logger) (Client, error)
	Preflight  func(ctx context.Context, cfg C) []PreflightCheck
	ToolDir    string
}

type Name string

func (n Name) Bit() uint32 {
	mu.RLock()
	defer mu.RUnlock()

	return protocolBits[n]
}

func (n Name) MarshalText() ([]byte, error) {
	if !namePattern.MatchString(string(n)) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidName, string(n))
	}

	return []byte(n), nil
}

func (n Name) String() string {
	return string(n)
}

func (n *Name) UnmarshalText(text []byte) error {
	parsed, err := ParseName(string(text))
	if err != nil {
		return err
	}

	*n = parsed

	return nil
}

type PeerID string

type PeerRequest struct {
	Name    Name            `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (r PeerRequest) Unpack(target any) error {
	if err := json.Unmarshal(r.Payload, target); err != nil {
		return fmt.Errorf("decoding peer request: %w", err)
	}

	return nil
}

type PeerResponse struct {
	ID      PeerID          `json:"id"`
	Name    Name            `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (r PeerResponse) Unpack(target any) error {
	if err := json.Unmarshal(r.Payload, target); err != nil {
		return fmt.Errorf("decoding peer response: %w", err)
	}

	return nil
}

type PeerRestorer interface {
	RestorePeer(ctx context.Context, req PeerRequest, resp PeerResponse) error
}

// LeftoverStatsReader is a server whose peers can outlive the daemon: a kernel WireGuard interface
// keeps forwarding traffic after the process dies. LeftoverPeerStats reads the counters of the
// interface a previous run left behind, before Start replaces it, so that traffic can still be
// metered.
type LeftoverStatsReader interface {
	LeftoverPeerStats(ctx context.Context) (map[PeerID]PeerStats, error)
}

type PeerStats struct {
	RxBytes   uint64 `json:"rx_bytes"`
	TxBytes   uint64 `json:"tx_bytes"`
	UpdatedAt int64  `json:"updated_at"`
}

type Placement struct {
	Dir       string
	Interface string
	Table     string
	ToolDir   string
}

func (p Placement) ToolPath(suffix string) string {
	return filepath.Join(p.ToolDir, p.Interface+suffix)
}

func (p Placement) validate() error {
	if err := ValidateInterfaceName(p.Interface); err != nil {
		return err
	}

	if !filepath.IsAbs(p.ToolDir) {
		return fmt.Errorf("%w: tool folder %q must be absolute", validate.ErrInvalidConfig, p.ToolDir)
	}

	return nil
}

type PreflightCheck struct {
	Detail   string   `json:"detail"`
	Name     string   `json:"name"`
	OK       bool     `json:"ok"`
	Severity Severity `json:"severity"`
}

func NewPreflightCheck(name string, severity Severity, detail string) PreflightCheck {
	return PreflightCheck{
		Detail:   detail,
		Name:     name,
		OK:       severity != SeverityError,
		Severity: severity,
	}
}

type Server interface {
	AddPeer(ctx context.Context, req PeerRequest) (PeerResponse, error)
	Metadata() ServiceMetadata
	Name() Name
	PeerStats(ctx context.Context, id PeerID) (PeerStats, error)
	RemovePeer(ctx context.Context, id PeerID) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Wait(ctx context.Context) error
}

func NewServer(name Name, cfg any, p Placement, log zerolog.Logger) (Server, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}

	mu.RLock()
	defer mu.RUnlock()

	entry, err := lookup(servers, name)
	if err != nil {
		return nil, err
	}

	return entry.construct(cfg, p, log)
}

type ServerDriver[C any] struct {
	Binaries   []string
	Bit        uint32
	Cleanup    func(ctx context.Context, p Placement) error
	Defaults   func() C
	LookupDirs func() []string
	New        func(cfg C, p Placement, log zerolog.Logger) (Server, error)
	Preflight  func(ctx context.Context, cfg C) []PreflightCheck
	ToolDir    string
}

type ServiceMetadata struct {
	Name    Name            `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (m ServiceMetadata) Unpack(target any) error {
	if err := json.Unmarshal(m.Payload, target); err != nil {
		return fmt.Errorf("decoding service metadata: %w", err)
	}

	return nil
}

type Severity string

type StopOnce struct {
	err  error
	once sync.Once
}

func (s *StopOnce) Do(stop func() error) error {
	s.once.Do(func() {
		s.err = stop()
	})

	return s.err
}

type Validator interface {
	Validate() error
}

type entry[T any] struct {
	binaries  []string
	cleanup   func(ctx context.Context, p Placement) error
	construct func(cfg any, p Placement, log zerolog.Logger) (T, error)
	defaults  func() any
	dirs      func() []string
	metadata  func() Validator
	preflight func(ctx context.Context, cfg any) ([]PreflightCheck, error)
	toolDir   string
	zero      func() any
}

func newEntry[C, T any](
	binaries []string,
	defaults func() C,
	dirs func() []string,
	construct func(C, Placement, zerolog.Logger) (T, error),
	preflight func(context.Context, C) []PreflightCheck,
) entry[T] {
	typed := func(cfg any, p Placement, log zerolog.Logger) (T, error) {
		var (
			want C
			none T
		)

		switch value := cfg.(type) {
		case *C:
			return construct(*value, p, log)
		case C:
			return construct(value, p, log)
		default:
			return none, fmt.Errorf("%w: want %T, got %T", ErrUnexpectedConfigType, want, cfg)
		}
	}

	typedPreflight := func(ctx context.Context, cfg any) ([]PreflightCheck, error) {
		if preflight == nil {
			return nil, nil
		}

		var want C

		switch value := cfg.(type) {
		case *C:
			return preflight(ctx, *value), nil
		case C:
			return preflight(ctx, value), nil
		default:
			return nil, fmt.Errorf("%w: want %T, got %T", ErrUnexpectedConfigType, want, cfg)
		}
	}

	anyDefaults := func() any { return defaults() }
	zero := func() any { return new(C) }

	return entry[T]{
		binaries:  binaries,
		construct: typed,
		defaults:  anyDefaults,
		dirs:      dirs,
		preflight: typedPreflight,
		zero:      zero,
	}
}

func Binaries(name Name) []string {
	mu.RLock()
	defer mu.RUnlock()

	var out []string

	if entry, ok := servers[name]; ok {
		out = append(out, entry.binaries...)
	}

	if entry, ok := clients[name]; ok {
		for _, binary := range entry.binaries {
			if !slices.Contains(out, binary) {
				out = append(out, binary)
			}
		}
	}

	return out
}

func CleanupServer(ctx context.Context, name Name, p Placement) error {
	if err := p.validate(); err != nil {
		return err
	}

	mu.RLock()

	entry, err := lookup(servers, name)

	mu.RUnlock()

	if err != nil {
		return err
	}

	if entry.cleanup == nil {
		return nil
	}

	return entry.cleanup(ctx, p)
}

func ClientToolDir(name Name) (string, error) {
	mu.RLock()
	defer mu.RUnlock()

	entry, err := lookup(clients, name)
	if err != nil {
		return "", err
	}

	return entry.toolDir, nil
}

func DefaultClientConfig(name Name) (any, error) {
	mu.RLock()
	defer mu.RUnlock()

	entry, err := lookup(clients, name)
	if err != nil {
		return nil, err
	}

	return entry.defaults(), nil
}

func DefaultServerConfig(name Name) (any, error) {
	mu.RLock()
	defer mu.RUnlock()

	entry, err := lookup(servers, name)
	if err != nil {
		return nil, err
	}

	return entry.defaults(), nil
}

func EmptyClientConfig(name Name) (any, error) {
	mu.RLock()
	defer mu.RUnlock()

	entry, err := lookup(clients, name)
	if err != nil {
		return nil, err
	}

	return entry.zero(), nil
}

func EmptyServerConfig(name Name) (any, error) {
	mu.RLock()
	defer mu.RUnlock()

	entry, err := lookup(servers, name)
	if err != nil {
		return nil, err
	}

	return entry.zero(), nil
}

func LoopbackAddr(port uint16) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port)))
}

func Names() []Name {
	mu.RLock()
	defer mu.RUnlock()

	out := make([]Name, 0, len(protocolBits))
	for name := range protocolBits {
		out = append(out, name)
	}

	slices.Sort(out)

	return out
}

func PackMetadata(name Name, payload any) (ServiceMetadata, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return ServiceMetadata{}, fmt.Errorf("encoding service metadata: %w", err)
	}

	return ServiceMetadata{
		Name:    name,
		Payload: raw,
	}, nil
}

func PackRequest(name Name, payload any) (PeerRequest, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return PeerRequest{}, fmt.Errorf("encoding peer request: %w", err)
	}

	return PeerRequest{
		Name:    name,
		Payload: raw,
	}, nil
}

func PackResponse(name Name, id PeerID, payload any) (PeerResponse, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return PeerResponse{}, fmt.Errorf("encoding peer response: %w", err)
	}

	return PeerResponse{
		ID:      id,
		Name:    name,
		Payload: raw,
	}, nil
}

func ParseName(text string) (Name, error) {
	mu.RLock()
	defer mu.RUnlock()

	name := Name(text)
	if _, ok := protocolBits[name]; !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownName, text)
	}

	return name, nil
}

func PreflightClient(ctx context.Context, name Name, cfg any) ([]PreflightCheck, error) {
	mu.RLock()

	entry, err := lookup(clients, name)

	mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return entry.preflight(ctx, cfg)
}

func PreflightServer(ctx context.Context, name Name, cfg any) ([]PreflightCheck, error) {
	mu.RLock()

	entry, err := lookup(servers, name)

	mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return entry.preflight(ctx, cfg)
}

func RegisterClient[C any](name Name, driver ClientDriver[C]) {
	if driver.Metadata == nil {
		panic(fmt.Errorf("registering %q: client metadata hook is required", string(name)))
	}

	value := newEntry(driver.Binaries, driver.Defaults, driver.LookupDirs, driver.New, driver.Preflight)
	value.metadata = driver.Metadata
	value.toolDir = driver.ToolDir

	register(name, driver.Bit, "client", clients, value)
}

func RegisterServer[C any](name Name, driver ServerDriver[C]) {
	value := newEntry(driver.Binaries, driver.Defaults, driver.LookupDirs, driver.New, driver.Preflight)
	value.cleanup = driver.Cleanup
	value.toolDir = driver.ToolDir

	register(name, driver.Bit, "server", servers, value)
}

func ScopeLogger(log zerolog.Logger, name Name) zerolog.Logger {
	return log.With().Str("service", name.String()).Logger()
}

func ServerToolDir(name Name) (string, error) {
	mu.RLock()
	defer mu.RUnlock()

	entry, err := lookup(servers, name)
	if err != nil {
		return "", err
	}

	return entry.toolDir, nil
}

func ToolDirs() []string {
	mu.RLock()
	defer mu.RUnlock()

	dirs := map[string]struct{}{}

	for _, client := range clients {
		dirs[client.toolDir] = struct{}{}
	}

	for _, server := range servers {
		dirs[server.toolDir] = struct{}{}
	}

	delete(dirs, "")

	return slices.Sorted(maps.Keys(dirs))
}

func UnpackMetadata[P Validator](md ServiceMetadata) (P, error) {
	return unpackValidated[P](md.Unpack, "service metadata", validate.ErrInvalidConfig)
}

func UnpackRequest[P Validator](req PeerRequest) (P, error) {
	return unpackValidated[P](req.Unpack, "peer request", ErrInvalidPeer)
}

func UnpackResponse[P Validator](resp PeerResponse) (P, error) {
	return unpackValidated[P](resp.Unpack, "peer response", ErrInvalidPeer)
}

func ValidateHostname(host string) error {
	if len(host) == 0 || len(host) > maxHostnameBytes {
		return fmt.Errorf("%w: length %d", ErrInvalidHostname, len(host))
	}

	for label := range strings.SplitSeq(host, ".") {
		if !labelPattern.MatchString(label) {
			return fmt.Errorf("%w: label %q", ErrInvalidHostname, label)
		}
	}

	return nil
}

func ValidateMetadata(md ServiceMetadata) error {
	mu.RLock()

	entry, err := lookup(clients, md.Name)

	mu.RUnlock()

	if err != nil {
		return err
	}

	target := entry.metadata()

	if err := md.Unpack(target); err != nil {
		return err
	}

	return target.Validate()
}

func ValidateUUID(name string) error {
	if _, err := uuid.Parse(name); err != nil {
		return fmt.Errorf("%w: uuid: %w", ErrInvalidPeer, err)
	}

	return nil
}

func lookup[T any](entries map[Name]entry[T], name Name) (entry[T], error) {
	found, ok := entries[name]
	if !ok {
		return entry[T]{}, fmt.Errorf("%w: %s", ErrUnknownName, name)
	}

	return found, nil
}

func register[T any](name Name, bit uint32, kind string, entries map[Name]entry[T], value entry[T]) {
	mu.Lock()
	defer mu.Unlock()

	if !namePattern.MatchString(string(name)) {
		panic(fmt.Errorf("registering %q: name must match %s", string(name), namePattern))
	}

	if bits.OnesCount32(bit) != 1 {
		panic(fmt.Errorf("registering %q: bit %#x must have exactly one bit set", string(name), bit))
	}

	if bound, ok := protocolBits[name]; ok && bound != bit {
		panic(fmt.Errorf("registering %q: bit %#x conflicts with the registered bit %#x", string(name), bit, bound))
	}

	for owner, bound := range protocolBits {
		if bound == bit && owner != name {
			panic(fmt.Errorf("registering %q: bit %#x is bound to %q", string(name), bit, string(owner)))
		}
	}

	if _, ok := entries[name]; ok {
		panic(fmt.Errorf("registering %q: %s already registered", string(name), kind))
	}

	protocolBits[name] = bit
	entries[name] = value
}

func registeredDirs() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]Name, 0, len(protocolBits))
	for name := range protocolBits {
		names = append(names, name)
	}

	slices.Sort(names)

	var out []string

	for _, name := range names {
		for _, dirs := range []func() []string{clients[name].dirs, servers[name].dirs} {
			if dirs == nil {
				continue
			}

			for _, dir := range dirs() {
				if !slices.Contains(out, dir) {
					out = append(out, dir)
				}
			}
		}
	}

	return out
}

func unpackValidated[P Validator](unpack func(any) error, label string, invalid error) (P, error) {
	var (
		none P
		p    P
	)

	if err := unpack(&p); err != nil {
		return none, err
	}

	if v := reflect.ValueOf(p); v.Kind() == reflect.Pointer && v.IsNil() {
		return none, fmt.Errorf("%w: %s: null payload", invalid, label)
	}

	if err := p.Validate(); err != nil {
		return none, err
	}

	return p, nil
}
