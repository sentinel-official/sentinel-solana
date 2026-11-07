package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/solana-foundation/solana-go/v2/rpc/jsonrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/rpcpin"
	"github.com/sentinel-official/sentinel-dvpnd/internal/store"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const (
	grpcBufSize                = 1 << 20
	otherFakeBit  uint32       = 1 << 29
	otherFakeName service.Name = "other"
)

//nolint:gochecknoglobals
var (
	defaultCleanups  = &recordingCleanups{}
	registerFakeOnce sync.Once
)

var (
	errDNSApplyFailed = errors.New("dns backend refused the servers")
	errDNSBlocked     = errors.New("dns blocked by the kill switch")
	errNoSuchHost     = errors.New("no such host")
	errRPCCutOff      = errors.New("rpc cut off by the kill switch")
)

type clientHooks struct {
	onCleanup      *func(string)
	onCleanupError *func() error
	onPeerRequest  *func()
	onStart        *func(context.Context)
	onStats        *func()
	onStatsError   *func() error
}

type dnsCall struct{ iface string }

type fakeCall struct {
	args  []string
	input string
	name  string
}

type fakeClientConfig struct {
	hooks clientHooks
	state *sdktest.FakeState
}

type fakeHookedClient struct {
	*sdktest.FakeClient

	dir   string
	hooks clientHooks
}

func (c *fakeHookedClient) Cleanup(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fake client cleanup: %w", err)
	}

	if c.hooks.onCleanup != nil {
		(*c.hooks.onCleanup)(c.dir)
	}

	if c.hooks.onCleanupError != nil {
		return (*c.hooks.onCleanupError)()
	}

	return nil
}

func (c *fakeHookedClient) PeerRequest(ctx context.Context) (service.PeerRequest, error) {
	if c.hooks.onPeerRequest != nil {
		(*c.hooks.onPeerRequest)()
	}

	return c.FakeClient.PeerRequest(ctx)
}

func (c *fakeHookedClient) Start(ctx context.Context, host string, resp service.PeerResponse) error {
	if c.hooks.onStart != nil {
		(*c.hooks.onStart)(ctx)
	}

	return c.FakeClient.Start(ctx, host, resp)
}

func (c *fakeHookedClient) Stats(ctx context.Context) (service.PeerStats, error) {
	stats, err := c.FakeClient.Stats(ctx)
	if c.hooks.onStats != nil {
		(*c.hooks.onStats)()
	}

	if c.hooks.onStatsError != nil {
		return stats, (*c.hooks.onStatsError)()
	}

	return stats, err
}

type fakeKillSwitchNet struct {
	admitted atomic.Pointer[[]netip.AddrPort]
	base     http.RoundTripper
}

func (n *fakeKillSwitchNet) RoundTrip(req *http.Request) (*http.Response, error) {
	if admitted := n.admitted.Load(); admitted != nil {
		dest, err := netip.ParseAddrPort(req.URL.Host)
		if err != nil || !slices.Contains(*admitted, dest) {
			return nil, fmt.Errorf("%w: %s", errRPCCutOff, req.URL.Host)
		}
	}

	resp, err := n.base.RoundTrip(req)
	if err != nil {
		return nil, fmt.Errorf("kill switch net: %w", err)
	}

	return resp, nil
}

func (n *fakeKillSwitchNet) apply(call firewallCall) {
	admitted := slices.Clone(call.rpcAndWSAddrs)
	n.admitted.Store(&admitted)
}

func (n *fakeKillSwitchNet) flush() {
	n.admitted.Store(nil)
}

func (n *fakeKillSwitchNet) lookup(_ context.Context, _, host string) ([]netip.Addr, error) {
	if n.admitted.Load() != nil {
		return nil, errDNSBlocked
	}

	addr, err := netip.ParseAddr(host)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errNoSuchHost, host)
	}

	return []netip.Addr{addr}, nil
}

func (n *fakeKillSwitchNet) programFor(
	t *testing.T,
	fakeRPC *sdktest.FakeRPC,
	current *program.Client,
) *program.Client {
	t.Helper()

	httpClient := &http.Client{Transport: n}
	opts := &jsonrpc.RPCClientOpts{HTTPClient: httpClient}
	rpcClient := jsonrpc.NewClientWithOpts(fakeRPC.Server.URL, opts)
	custom := rpc.NewWithCustomRPCClient(rpcClient)

	return program.NewClient(custom, current.ProgramID, current.Mint, current.Commitment)
}

type fakeNodeResponse struct {
	body   []byte
	status int
}

func (f fakeNodeResponse) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		Body:       io.NopCloser(bytes.NewReader(f.body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Request:    req,
		StatusCode: f.status,
	}, nil
}

type fakePFRollback struct {
	bounded    bool
	cancel     context.CancelFunc
	contextErr error
	err        error
	onEnable   func()
	released   bool
}

func (f *fakePFRollback) run(ctx context.Context, _, _ string, args ...string) ([]byte, error) {
	switch args[0] {
	case "-v":
		return []byte(pfCleanInterfaces), nil
	case "-E":
		if f.cancel != nil {
			f.cancel()
		}

		if f.onEnable != nil {
			f.onEnable()
		}

		return []byte("Token : 42"), nil
	case "-X":
		f.contextErr = ctx.Err()
		_, f.bounded = ctx.Deadline()
		f.released = len(args) == 2 && args[1] == "42"

		return nil, f.err
	default:
		return nil, nil
	}
}

type fakeRun struct {
	calls   []fakeCall
	errs    map[string]error
	outputs map[string]string
}

func (f *fakeRun) commands() []string {
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.name+" "+strings.Join(c.args, " "))
	}

	return out
}

func (f *fakeRun) run(_ context.Context, input, name string, args ...string) ([]byte, error) {
	key := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, fakeCall{args: args, input: input, name: name})

	if err := f.errs[key]; err != nil {
		return []byte(err.Error()), err
	}

	return []byte(f.outputs[key]), nil
}

type firewallCall struct {
	iface         string
	nodeAddrs     []netip.Addr
	rpcAndWSAddrs []netip.AddrPort
}

type killSwitchFixture struct {
	dnsBackend    *recordingDNSBackend
	firewall      *recordingFirewallBackend
	home          testutil.Home
	ks            *fakeKillSwitchNet
	programClient *program.Client
	req           *daemonv1.StartSessionRequest
	rt            *client.Runtime
	state         *sdktest.FakeState
}

func newKillSwitchFixture(t *testing.T) killSwitchFixture {
	t.Helper()
	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	home.Loaded.Client.Network.KillSwitch = config.KillSwitchPersist
	ks := &fakeKillSwitchNet{base: http.DefaultTransport}
	programClient := ks.programFor(t, fakeRPC, home.ProgramClient)
	rt := newRuntime(t, home, programClient)
	client.SetLookup(rt, ks.lookup)
	firewall := &recordingFirewallBackend{onApply: ks.apply, onFlush: ks.flush}
	dns := &recordingDNSBackend{}

	client.SetFirewallBackend(rt, firewall)
	client.SetDNSBackend(rt, dns)
	registerFakeClientOnce()

	state := sdktest.NewFakeState(sdktest.FakeName)
	state.SetLocalStats(service.PeerStats{RxBytes: 1})
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{state: state})

	req := seedTicketRequest(t, fakeRPC, home, home.DaemonKey.PublicKey(), time.Now().Add(time.Hour).Unix())

	return killSwitchFixture{
		dnsBackend:    dns,
		firewall:      firewall,
		home:          home,
		ks:            ks,
		programClient: programClient,
		req:           req,
		rt:            rt,
		state:         state,
	}
}

func (f killSwitchFixture) startAndSettle(t *testing.T, ctx context.Context) {
	t.Helper()

	sess, err := client.StartSession(ctx, f.rt, f.req)
	require.NoError(t, err)
	waitTicked(t, ctx, f.home.DB, sess.ID)
}

func (f killSwitchFixture) stopOnFirstStart() <-chan error {
	starts := 0
	stopped := make(chan error, 1)
	stop := func(ctx context.Context) {
		starts++
		if starts != 1 {
			return
		}

		go func() { stopped <- client.StopSession(ctx, f.rt) }()

		<-ctx.Done()
	}
	client.SetService(f.rt, sdktest.FakeName, fakeClientConfig{
		hooks: clientHooks{onStart: &stop}, state: f.state,
	})

	return stopped
}

type pendingSeed struct {
	account sdktest.Account
	address solana.PublicKey
}

type pendingSeeds struct {
	items []pendingSeed
	mu    sync.Mutex
}

func (p *pendingSeeds) drain() []pendingSeed {
	p.mu.Lock()
	defer p.mu.Unlock()

	items := p.items
	p.items = nil

	return items
}

func (p *pendingSeeds) push(address solana.PublicKey, account sdktest.Account) {
	p.mu.Lock()
	defer p.mu.Unlock()

	item := pendingSeed{
		account: account,
		address: address,
	}
	p.items = append(p.items, item)
}

type recordingCleanups struct {
	cleaned []string
	mu      sync.Mutex
}

func (c *recordingCleanups) cleanups() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.cleaned)
}

func (c *recordingCleanups) record(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleaned = append(c.cleaned, dir)
}

type recordingDNSBackend struct {
	applied    []dnsCall
	cleanupErr error
	cleanups   int
	failFrom   int
	mu         sync.Mutex
	reverted   []string
	revertErr  error
}

func (b *recordingDNSBackend) Apply(ctx context.Context, iface string, _ []string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("recording dns apply: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.applied = append(b.applied, dnsCall{iface: iface})
	if b.failFrom > 0 && len(b.applied) >= b.failFrom {
		return errDNSApplyFailed
	}

	return nil
}

func (b *recordingDNSBackend) Cleanup(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("recording dns cleanup: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.cleanups++

	return b.cleanupErr
}

func (b *recordingDNSBackend) Revert(ctx context.Context, iface string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("recording dns revert: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.reverted = append(b.reverted, iface)

	return b.revertErr
}

func (b *recordingDNSBackend) applies() []dnsCall {
	b.mu.Lock()
	defer b.mu.Unlock()

	return slices.Clone(b.applied)
}

func (b *recordingDNSBackend) cleanupCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.cleanups
}

func (b *recordingDNSBackend) reverts() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return slices.Clone(b.reverted)
}

type recordingFirewallBackend struct {
	applied  []firewallCall
	applyErr error
	flushed  int
	flushErr error
	mu       sync.Mutex
	onApply  func(firewallCall)
	onFlush  func()
}

func (f *recordingFirewallBackend) Apply(
	ctx context.Context,
	iface string,
	nodeAddrs []netip.Addr,
	rpcAndWSAddrs []netip.AddrPort,
	_ bool,
) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("recording firewall apply: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	call := firewallCall{iface: iface, nodeAddrs: slices.Clone(nodeAddrs), rpcAndWSAddrs: slices.Clone(rpcAndWSAddrs)}

	f.applied = append(f.applied, call)
	if f.onApply != nil {
		f.onApply(call)
	}

	return f.applyErr
}

func (f *recordingFirewallBackend) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("recording firewall flush: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.flushed++
	if f.onFlush != nil && f.flushErr == nil {
		f.onFlush()
	}

	return f.flushErr
}

func (f *recordingFirewallBackend) applies() []firewallCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.applied)
}

func (f *recordingFirewallBackend) flushes() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.flushed
}

type seedingTransport struct {
	base    http.RoundTripper
	fakeRPC *sdktest.FakeRPC
	pending *pendingSeeds
}

func (s *seedingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for _, item := range s.pending.drain() {
		s.fakeRPC.SetAccount(item.address, item.account)
	}

	resp, err := s.base.RoundTrip(req)
	if err != nil {
		return nil, fmt.Errorf("seeding transport: %w", err)
	}

	return resp, nil
}

type ticketFixture struct {
	firewall *recordingFirewallBackend
	home     testutil.Home
	owner    solana.PublicKey
	rpc      *sdktest.FakeRPC
	rt       *client.Runtime
	state    *sdktest.FakeState
}

func newTicketFixture(t *testing.T) ticketFixture {
	t.Helper()

	ownerKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	home.Loaded.Client.Owner = ownerKey.PublicKey().String()
	rt := newRuntime(t, home, home.ProgramClient)

	firewall := &recordingFirewallBackend{}
	client.SetFirewallBackend(rt, firewall)
	client.SetDNSBackend(rt, &recordingDNSBackend{})

	registerFakeClientOnce()

	state := sdktest.NewFakeState(sdktest.FakeName)
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{state: state})

	return ticketFixture{
		firewall: firewall,
		home:     home,
		rt:       rt,
		owner:    ownerKey.PublicKey(),
		rpc:      fakeRPC,
		state:    state,
	}
}

func awaitErr(t *testing.T, ctx context.Context, ch <-chan error) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for a result")

		return nil
	}
}

func fakePFCommand() int {
	_, _ = fmt.Fprintln(os.Stderr, "test pf diagnostic")

	switch strings.Join(os.Args[1:], " ") {
	case "-v -s Interfaces":
		_, _ = fmt.Fprint(os.Stdout, "lo0 (skip)\nen0\nutun3\n")
	case "-sr":
		_, _ = fmt.Fprint(os.Stdout, os.Getenv("SENTINEL_PF_ROOT"))
	case "-a sentinel-dvpn-c0 -sr":
		_, _ = fmt.Fprint(os.Stdout, "block drop out all\npass out quick on lo0 all\n")
	case "-s info":
		_, _ = fmt.Fprint(os.Stdout, "Status: Enabled\n")
	case "-E":
		_, _ = fmt.Fprintln(os.Stderr, "Token : 42")
	case "-R -f -":
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			return 1
		}

		if os.WriteFile(os.Getenv("SENTINEL_PF_RELOAD"), input, 0o600) != nil { //#nosec G703 -- Test fixture path.
			return 1
		}
	case "fail":
		return 1
	}

	return 0
}

func firewallRows(t *testing.T, home testutil.Home) []client.Firewall {
	t.Helper()

	var rows []client.Firewall
	require.NoError(t, home.DB.WithContext(t.Context()).Find(&rows).Error)

	return rows
}

func installOpenChannelReactor(t *testing.T, fakeRPC *sdktest.FakeRPC, home testutil.Home) *program.Client {
	t.Helper()

	programID := home.ProgramClient.ProgramID
	device := home.DaemonKey.PublicKey()
	node := home.NodeAddress
	escrow := home.Loaded.Client.EscrowBaseUnits

	pending := &pendingSeeds{}

	fakeRPC.Simulate = func(txn *solana.Transaction) sdktest.SimulateResult {
		for _, ci := range txn.Message.Instructions {
			data := []byte(ci.Data)
			if !bytes.HasPrefix(data, dvpn.OpenChannelInstructionDiscriminator[:]) {
				continue
			}

			metas, err := ci.ResolveInstructionAccounts(&txn.Message)
			if err != nil || len(metas) < 7 {
				continue
			}

			ch := dvpn.Channel{
				Escrow:        escrow,
				Mode:          dvpn.PositionModeVoucher,
				Node:          node,
				Opener:        device,
				Owner:         home.OwnerAddress,
				VoucherSigner: device,
			}
			account := sdktest.Account{
				Data:  sdktest.EncodeAccount(dvpn.ChannelAccountDiscriminator, ch),
				Owner: programID,
			}
			pending.push(metas[6].PublicKey, account)
		}

		return sdktest.SimulateResult{UnitsConsumed: 5_000}
	}

	transport := &seedingTransport{
		base:    http.DefaultTransport,
		pending: pending,
		fakeRPC: fakeRPC,
	}
	httpClient := &http.Client{Transport: transport}
	opts := &jsonrpc.RPCClientOpts{HTTPClient: httpClient}
	rpcClient := jsonrpc.NewClientWithOpts(fakeRPC.Server.URL, opts)
	custom := rpc.NewWithCustomRPCClient(rpcClient)

	return program.NewClient(custom, programID, home.ProgramClient.Mint, home.ProgramClient.Commitment)
}

func newChannelRequest(node string) *daemonv1.StartSessionRequest {
	newChannel := &daemonv1.NewChannel{Mode: uint32(dvpn.PositionModeVoucher)}
	credential := &daemonv1.Credential{Kind: &daemonv1.Credential_NewChannel{NewChannel: newChannel}}

	return &daemonv1.StartSessionRequest{
		Credential: credential,
		Node:       node,
	}
}

func newRuntime(t *testing.T, home testutil.Home, programClient *program.Client) *client.Runtime {
	t.Helper()
	registerFakeClientOnce()

	r, err := client.New(
		home.Loaded, home.DB, home.DaemonKey, programClient, home.Sender, zerolog.Nop(), prometheus.NewRegistry(), home.Pin,
	)
	require.NoError(t, err)
	require.NoError(t, store.Migrate(t.Context(), home.DB, r.Models()...))
	client.SetInterfaceRemover(r, &testutil.RecordingInterfaceRemover{})
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	return r
}

func newWorkingRuntime(t *testing.T) (*client.Runtime, testutil.Home, *sdktest.FakeState) {
	t.Helper()
	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	rt := newRuntime(t, home, installOpenChannelReactor(t, fakeRPC, home))
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	registerFakeClientOnce()

	state := sdktest.NewFakeState(sdktest.FakeName)
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{state: state})
	require.NoError(t, rt.Recover(t.Context()))

	return rt, home, state
}

func registerFakeClient() {
	newClient := func(cfg fakeClientConfig, p service.Placement, _ zerolog.Logger) (service.Client, error) {
		fake := sdktest.NewFakeClient(cfg.state)
		if cfg.hooks == (clientHooks{}) {
			return fake, nil
		}

		return &fakeHookedClient{FakeClient: fake, dir: p.Dir, hooks: cfg.hooks}, nil
	}
	driver := service.ClientDriver[fakeClientConfig]{
		Bit:      sdktest.FakeBit,
		Defaults: func() fakeClientConfig { return fakeClientConfig{} },
		Metadata: func() service.Validator { return &sdktest.FakeMetadata{} },
		New:      newClient,
	}
	service.RegisterClient(sdktest.FakeName, driver)

	record := defaultCleanups.record
	defaults := fakeClientConfig{hooks: clientHooks{onCleanup: &record}}
	driver.Bit = otherFakeBit
	driver.Defaults = func() fakeClientConfig { return defaults }
	service.RegisterClient(otherFakeName, driver)
}

func registerFakeClientOnce() {
	registerFakeOnce.Do(registerFakeClient)
}

func requirePinned(t *testing.T, pin *rpcpin.Pin, want []netip.AddrPort) {
	t.Helper()

	pinned, active := pin.Pinned()
	require.True(t, active)
	require.Equal(t, want, pinned)
}

func requireUnpinned(t *testing.T, pin *rpcpin.Pin) {
	t.Helper()

	_, active := pin.Pinned()
	require.False(t, active)
}

func restartRuntime(
	t *testing.T,
	ctx context.Context,
	home testutil.Home,
	programClient *program.Client,
	firewall *recordingFirewallBackend,
	lookup func(context.Context, string, string) ([]netip.Addr, error),
	dns *recordingDNSBackend,
) (*client.Runtime, *rpcpin.Pin) {
	t.Helper()

	pin, err := rpcpin.New(home.Loaded.File.RPC.URL, home.Loaded.File.RPC.Dialer())
	require.NoError(t, err)
	rt, err := client.New(
		home.Loaded, home.DB, home.DaemonKey, programClient, home.Sender, zerolog.Nop(), prometheus.NewRegistry(), pin,
	)
	require.NoError(t, err)
	client.SetLookup(rt, lookup)
	client.SetFirewallBackend(rt, firewall)
	client.SetDNSBackend(rt, dns)
	client.SetInterfaceRemover(rt, &testutil.RecordingInterfaceRemover{})
	_ = rt.Recover(ctx)

	return rt, pin
}

func rpcEndpoints(t *testing.T, home testutil.Home) ([]netip.AddrPort, []netip.AddrPort) {
	t.Helper()

	cfg := home.Loaded.File.RPC
	u, err := url.Parse(cfg.URL)
	require.NoError(t, err)
	rpcAddr, err := netip.ParseAddrPort(u.Host)
	require.NoError(t, err)
	u, err = url.Parse(cfg.WSURL)
	require.NoError(t, err)
	wsAddr, err := netip.ParseAddrPort(u.Host)
	require.NoError(t, err)

	rpcAndWSAddrs := []netip.AddrPort{rpcAddr, wsAddr}
	slices.SortFunc(rpcAndWSAddrs, netip.AddrPort.Compare)

	return slices.Compact(rpcAndWSAddrs), []netip.AddrPort{rpcAddr}
}

func seedTicketRequest(
	t *testing.T,
	fakeRPC *sdktest.FakeRPC,
	home testutil.Home,
	user solana.PublicKey,
	expiresUnix int64,
) *daemonv1.StartSessionRequest {
	t.Helper()

	providerOwner, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	providerKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	subscriptionKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	providerAddress := providerKey.PublicKey()
	subscription := subscriptionKey.PublicKey()

	provider := dvpn.Provider{Owner: providerOwner.PublicKey()}
	providerAcct := sdktest.Account{
		Data:  sdktest.EncodeAccount(dvpn.ProviderAccountDiscriminator, provider),
		Owner: home.ProgramClient.ProgramID,
	}
	fakeRPC.SetAccount(providerAddress, providerAcct)

	ticket := auth.Ticket{
		ExpiresUnix:  expiresUnix,
		IssuedUnix:   time.Now().Unix(),
		Node:         home.NodeAddress,
		Provider:     providerAddress,
		Subscription: subscription,
		User:         user,
		Version:      auth.TicketVersion,
	}
	signedTicket := auth.SignTicket(providerOwner, ticket)
	home.NodeService.SetTicket(signedTicket.Encode())

	ticketReq := &daemonv1.TicketRequest{
		Provider:     providerAddress.String(),
		Subscription: subscription.String(),
	}
	credential := &daemonv1.Credential{Kind: &daemonv1.Credential_TicketRequest{TicketRequest: ticketReq}}
	req := &daemonv1.StartSessionRequest{
		Credential: credential,
		Node:       home.NodeAddress.String(),
	}

	return req
}

func setNodeResponse(t *testing.T, rt *client.Runtime, status int, response any) {
	t.Helper()

	body, err := json.Marshal(response)
	require.NoError(t, err)
	nodeClient, err := nodeapi.NewClientWithResponses("https://example.com", nodeapi.WithHTTPClient(fakeNodeResponse{
		body:   body,
		status: status,
	}))
	require.NoError(t, err)
	client.SetNodeClient(rt, nodeClient)
}

func startClientService(t *testing.T, rt *client.Runtime) daemonv1.ClientServiceClient {
	t.Helper()

	lis := bufconn.Listen(grpcBufSize)
	srv := grpc.NewServer()
	rt.Register(srv)

	serve := func() { _ = srv.Serve(lis) }
	go serve()

	t.Cleanup(srv.Stop)

	dial := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	opts := []grpc.DialOption{grpc.WithContextDialer(dial), grpc.WithTransportCredentials(insecure.NewCredentials())}

	conn, err := grpc.NewClient("passthrough:///bufconn", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return daemonv1.NewClientServiceClient(conn)
}

func storedAddrPorts(t *testing.T, value string) []netip.AddrPort {
	t.Helper()

	result := make([]netip.AddrPort, 0)

	for part := range strings.FieldsSeq(value) {
		addr, err := netip.ParseAddrPort(part)
		require.NoError(t, err)

		result = append(result, addr)
	}

	return result
}

func storedAddrs(t *testing.T, value string) []netip.Addr {
	t.Helper()

	result := make([]netip.Addr, 0)

	for part := range strings.FieldsSeq(value) {
		addr, err := netip.ParseAddr(part)
		require.NoError(t, err)

		result = append(result, addr)
	}

	return result
}

func ticketCount(t *testing.T, home testutil.Home) int64 {
	t.Helper()

	var count int64
	require.NoError(t, home.DB.WithContext(t.Context()).Model(&client.Ticket{}).Count(&count).Error)

	return count
}

func waitTicked(t *testing.T, ctx context.Context, db *gorm.DB, sessionID string) {
	t.Helper()

	for {
		var row client.Session
		require.NoError(t, db.WithContext(ctx).First(&row, "id = ?", sessionID).Error)

		if row.RxBytes > 0 {
			return
		}

		select {
		case <-ctx.Done():
			require.FailNow(t, "the issuer never ticked")

			return
		case <-time.After(time.Millisecond):
		}
	}
}
