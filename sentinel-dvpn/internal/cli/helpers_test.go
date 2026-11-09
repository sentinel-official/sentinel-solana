package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentinel-official/sentinel-dvpn/internal/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/providerapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

//nolint:gochecknoglobals
var (
	otherMint = solana.PublicKey{2}
	testMint  = solana.PublicKey{1}
)

type fakeClientService struct {
	daemonv1.UnimplementedClientServiceServer
}

func (fakeClientService) GetSession(
	_ context.Context,
	req *daemonv1.GetSessionRequest,
) (*daemonv1.GetSessionResponse, error) {
	if err := checkID(req.GetId(), "sess-1"); err != nil {
		return nil, err
	}

	session := &daemonv1.Session{
		Id:            "sess-1",
		Node:          "node-addr",
		ProxyEndpoint: "127.0.0.1:1080",
		Status:        "active",
	}

	return &daemonv1.GetSessionResponse{Session: session}, nil
}

func (fakeClientService) ListChannels(
	context.Context,
	*daemonv1.ListChannelsRequest,
) (*daemonv1.ListChannelsResponse, error) {
	channel := &daemonv1.Channel{
		Address: "chan-1",
		Node:    "node-addr",
		Status:  "open",
	}

	return &daemonv1.ListChannelsResponse{Channels: []*daemonv1.Channel{channel}}, nil
}

func (fakeClientService) ListSessions(
	context.Context,
	*daemonv1.ListSessionsRequest,
) (*daemonv1.ListSessionsResponse, error) {
	session := &daemonv1.Session{
		Id:     "sess-1",
		Node:   "node-addr",
		Status: "active",
	}

	return &daemonv1.ListSessionsResponse{Sessions: []*daemonv1.Session{session}}, nil
}

func (fakeClientService) ListTickets(
	context.Context,
	*daemonv1.ListTicketsRequest,
) (*daemonv1.ListTicketsResponse, error) {
	ticket := &daemonv1.Ticket{
		Hash:     "hash-1",
		Provider: "prov-addr",
	}

	return &daemonv1.ListTicketsResponse{Tickets: []*daemonv1.Ticket{ticket}}, nil
}

func (fakeClientService) StopSession(
	_ context.Context,
	req *daemonv1.StopSessionRequest,
) (*daemonv1.StopSessionResponse, error) {
	if err := checkID(req.GetId(), "sess-1"); err != nil {
		return nil, err
	}

	if req.GetKeepKillSwitch() {
		// Echo the flag so the CLI test sees it reached the request.
		return &daemonv1.StopSessionResponse{Session: &daemonv1.Session{Id: "sess-1", Error: "keep_kill_switch"}}, nil
	}

	return &daemonv1.StopSessionResponse{}, nil
}

type fakeDaemonService struct {
	daemonv1.UnimplementedDaemonServiceServer
}

func (fakeDaemonService) GetVersion(
	context.Context,
	*daemonv1.GetVersionRequest,
) (*daemonv1.GetVersionResponse, error) {
	version := &daemonv1.Version{
		Commit:  "test-commit",
		Network: "test-network",
	}

	return &daemonv1.GetVersionResponse{Version: version}, nil
}

type fakeNodeAPI struct {
	nodeapi.Unimplemented

	healthRequests atomic.Int32
	metadata       auth.SignedMessage
}

func (f *fakeNodeAPI) GetHealth(
	context.Context,
	nodeapi.GetHealthRequestObject,
) (nodeapi.GetHealthResponseObject, error) {
	f.healthRequests.Add(1)

	resp := nodeapi.GetHealth200JSONResponse{
		Status:  "ok",
		Version: "test",
	}

	return resp, nil
}

func (f *fakeNodeAPI) GetMetadata(
	context.Context,
	nodeapi.GetMetadataRequestObject,
) (nodeapi.GetMetadataResponseObject, error) {
	resp := nodeapi.GetMetadata200JSONResponse(f.metadata)

	return resp, nil
}

type fakeNodeService struct {
	daemonv1.UnimplementedNodeServiceServer
}

func (fakeNodeService) CloseNodeSession(
	_ context.Context,
	req *daemonv1.CloseNodeSessionRequest,
) (*daemonv1.CloseNodeSessionResponse, error) {
	if err := checkID(req.GetId(), "ns-1"); err != nil {
		return nil, err
	}

	return &daemonv1.CloseNodeSessionResponse{}, nil
}

func (fakeNodeService) GetNodeInfo(
	context.Context,
	*daemonv1.GetNodeInfoRequest,
) (*daemonv1.GetNodeInfoResponse, error) {
	info := &daemonv1.NodeInfo{
		Address: "node-addr",
		Live:    true,
		Status:  "active",
	}

	return &daemonv1.GetNodeInfoResponse{Info: info}, nil
}

func (fakeNodeService) GetNodeSession(
	_ context.Context,
	req *daemonv1.GetNodeSessionRequest,
) (*daemonv1.GetNodeSessionResponse, error) {
	if err := checkID(req.GetId(), "ns-1"); err != nil {
		return nil, err
	}

	session := &daemonv1.NodeSession{
		Id:     "ns-1",
		Status: "active",
	}

	return &daemonv1.GetNodeSessionResponse{Session: session}, nil
}

func (fakeNodeService) HeartbeatNow(
	context.Context,
	*daemonv1.HeartbeatNowRequest,
) (*daemonv1.HeartbeatNowResponse, error) {
	return &daemonv1.HeartbeatNowResponse{}, nil
}

func (fakeNodeService) ListNodeSessions(
	context.Context,
	*daemonv1.ListNodeSessionsRequest,
) (*daemonv1.ListNodeSessionsResponse, error) {
	session := &daemonv1.NodeSession{
		Id:     "ns-1",
		Status: "active",
	}

	return &daemonv1.ListNodeSessionsResponse{Sessions: []*daemonv1.NodeSession{session}}, nil
}

func (fakeNodeService) ListPositions(
	context.Context,
	*daemonv1.ListPositionsRequest,
) (*daemonv1.ListPositionsResponse, error) {
	position := &daemonv1.Position{
		Address: "pos-1",
		Kind:    "channel",
	}

	return &daemonv1.ListPositionsResponse{Positions: []*daemonv1.Position{position}}, nil
}

func (fakeNodeService) ListRedemptions(
	context.Context,
	*daemonv1.ListRedemptionsRequest,
) (*daemonv1.ListRedemptionsResponse, error) {
	redemption := &daemonv1.Redemption{
		Signature: "sig-1",
		Status:    "confirmed",
	}

	return &daemonv1.ListRedemptionsResponse{Redemptions: []*daemonv1.Redemption{redemption}}, nil
}

func (fakeNodeService) RedeemNow(context.Context, *daemonv1.RedeemNowRequest) (*daemonv1.RedeemNowResponse, error) {
	return &daemonv1.RedeemNowResponse{Signatures: []string{"sig-1"}}, nil
}

func (fakeNodeService) SyncConfigNow(
	context.Context,
	*daemonv1.SyncConfigNowRequest,
) (*daemonv1.SyncConfigNowResponse, error) {
	return &daemonv1.SyncConfigNowResponse{}, nil
}

type fakeProviderAPI struct {
	providerapi.Unimplemented

	corruptSignature bool
	issuer           solana.PrivateKey
	metadata         auth.SignedMessage
}

func (f *fakeProviderAPI) GetMetadata(
	context.Context,
	providerapi.GetMetadataRequestObject,
) (providerapi.GetMetadataResponseObject, error) {
	resp := providerapi.GetMetadata200JSONResponse(f.metadata)

	return resp, nil
}

func (f *fakeProviderAPI) IssueTicket(
	_ context.Context,
	req providerapi.IssueTicketRequestObject,
) (providerapi.IssueTicketResponseObject, error) {
	signedRequest, err := auth.DecodeTicketRequest(req.Body.TicketRequest)
	if err != nil {
		return nil, fmt.Errorf("decoding ticket request: %w", err)
	}

	now := time.Now()
	ticket := auth.Ticket{
		DataCapBytes: 1_000_000,
		ExpiresUnix:  now.Add(time.Hour).Unix(),
		IssuedUnix:   now.Unix(),
		Node:         signedRequest.Request.Node,
		Provider:     signedRequest.Request.Provider,
		Subscription: signedRequest.Request.Subscription,
		User:         signedRequest.Signer,
		Version:      auth.TicketVersion,
	}

	signed := auth.SignTicket(f.issuer, ticket)
	if f.corruptSignature {
		signed.Signature[0] ^= 0xff
	}

	resp := providerapi.IssueTicket200JSONResponse{
		DataCapBytes: ticket.DataCapBytes,
		ExpiresAt:    ticket.ExpiresUnix,
		Ticket:       signed.Encode(),
	}

	return resp, nil
}

type fakeProviderService struct {
	daemonv1.UnimplementedProviderServiceServer
}

func (fakeProviderService) GetProviderInfo(
	context.Context,
	*daemonv1.GetProviderInfoRequest,
) (*daemonv1.GetProviderInfoResponse, error) {
	info := &daemonv1.ProviderInfo{
		Address: "prov-addr",
		Status:  "listed",
	}

	return &daemonv1.GetProviderInfoResponse{Info: info}, nil
}

func (fakeProviderService) KeepNow(context.Context, *daemonv1.KeepNowRequest) (*daemonv1.KeepNowResponse, error) {
	return &daemonv1.KeepNowResponse{}, nil
}

func (fakeProviderService) ListIssuedTickets(
	context.Context,
	*daemonv1.ListIssuedTicketsRequest,
) (*daemonv1.ListIssuedTicketsResponse, error) {
	ticket := &daemonv1.IssuedTicket{
		Hash: "hash-1",
		User: "user-addr",
	}

	return &daemonv1.ListIssuedTicketsResponse{Tickets: []*daemonv1.IssuedTicket{ticket}}, nil
}

func (fakeProviderService) ListLeases(
	context.Context,
	*daemonv1.ListLeasesRequest,
) (*daemonv1.ListLeasesResponse, error) {
	lease := &daemonv1.Lease{
		Address: "lease-1",
		Healthy: true,
		Status:  "open",
	}

	return &daemonv1.ListLeasesResponse{Leases: []*daemonv1.Lease{lease}}, nil
}

func (fakeProviderService) ListRosterNodes(
	context.Context,
	*daemonv1.ListRosterNodesRequest,
) (*daemonv1.ListRosterNodesResponse, error) {
	node := &daemonv1.RosterNode{
		Live: true,
		Node: "node-addr",
		Plan: "plan-addr",
	}

	return &daemonv1.ListRosterNodesResponse{Nodes: []*daemonv1.RosterNode{node}}, nil
}

func (fakeProviderService) ProbeNow(context.Context, *daemonv1.ProbeNowRequest) (*daemonv1.ProbeNowResponse, error) {
	return &daemonv1.ProbeNowResponse{}, nil
}

type recordingClientService struct {
	fakeClientService

	deadlines chan time.Time
}

func (r *recordingClientService) WatchSession(
	_ *daemonv1.WatchSessionRequest,
	stream grpc.ServerStreamingServer[daemonv1.WatchSessionResponse],
) error {
	ctx := stream.Context()

	deadline, _ := ctx.Deadline()
	r.deadlines <- deadline

	<-ctx.Done()

	return fmt.Errorf("watch canceled: %w", status.FromContextError(ctx.Err()).Err())
}

type recordingDaemonService struct {
	fakeDaemonService

	deadlines        chan time.Time
	waitCancellation bool
}

func (r *recordingDaemonService) GetVersion(
	ctx context.Context,
	req *daemonv1.GetVersionRequest,
) (*daemonv1.GetVersionResponse, error) {
	deadline, _ := ctx.Deadline()
	r.deadlines <- deadline

	if r.waitCancellation {
		<-ctx.Done()

		return nil, fmt.Errorf("version call canceled: %w", status.FromContextError(ctx.Err()).Err())
	}

	return r.fakeDaemonService.GetVersion(ctx, req)
}

type recordingPrinter struct {
	*cli.Printer

	last []byte
}

func newRecordingPrinter() *recordingPrinter {
	printer := &recordingPrinter{}
	printer.Printer = cli.NewPrinter(printer, cli.FormatJSON)

	return printer
}

func (r *recordingPrinter) Write(data []byte) (int, error) {
	r.last = append([]byte(nil), data...)

	return len(data), nil
}

func (r *recordingPrinter) decode(t *testing.T, value any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(r.last, value))
}

type runnerFixture struct {
	printer       *recordingPrinter
	programClient *program.Client
	runner        *cli.Runner
	sender        *tx.Sender
	signer        *cli.SignerKey
}

func newRunnerFixture(t *testing.T, fakeRPC *sdktest.FakeRPC) runnerFixture {
	t.Helper()
	privateKey := randomPrivateKey(t)

	programClient := program.NewClient(fakeRPC.NewClient(), dvpn.ProgramID, testMint, rpc.CommitmentConfirmed)
	sender := tx.NewSender(fakeRPC.NewClient(), nil, tx.DefaultConfig(), zerolog.Nop())
	printer := newRecordingPrinter()
	signer := cli.NewSignerKeyForTest(privateKey)

	return runnerFixture{
		signer:        signer,
		printer:       printer,
		programClient: programClient,
		runner:        cli.NewRunner(programClient, sender, signer, printer.Printer),
		sender:        sender,
	}
}

func checkID(got, want string) error {
	if got != want {
		return status.Errorf(codes.NotFound, "unknown id %q", got)
	}

	return nil
}

func clientArgs(args ...string) []string {
	return append([]string{"client"}, append(args, "--instance", "c0")...)
}

func findChainCmd(t *testing.T, runner *cli.Runner, group, name string) *cobra.Command {
	t.Helper()

	for _, candidate := range cli.NewChainGroups(runner) {
		if candidate.Use == group {
			cmd := findSubcommand(t, candidate, name)
			candidate.RemoveCommand(cmd)

			return cmd
		}
	}

	t.Fatalf("no %q group", group)

	return nil
}

func findSubcommand(t *testing.T, parent *cobra.Command, name string) *cobra.Command {
	t.Helper()

	for _, child := range parent.Commands() {
		if child.Name() == name {
			return child
		}
	}

	t.Fatalf("no %q subcommand under %q", name, parent.Name())

	return nil
}

func newRoot(t *testing.T, endpoint string, args ...string) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	sdktest.PinBuildIdentity(t, dvpn.ProgramID, testMint)

	root := cli.NewRootCmd()

	rootArgs := []string{
		"--home", t.TempDir(),
		"--rpc.url", "http://127.0.0.1:1",
		"--rpc.ws-url", "ws://127.0.0.1:1",
	}
	if endpoint != "" {
		rootArgs = append(rootArgs, "--daemon.endpoint", endpoint)
	}

	root.SetArgs(append(rootArgs, args...))

	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(io.Discard)

	return root, &buf
}

func nodeArgs(args ...string) []string {
	return append([]string{"node"}, append(args, "--instance", "n0")...)
}

func packURI(raw string) ([128]uint8, uint8) {
	var uri [128]uint8
	copy(uri[:], raw)

	return uri, uint8(len(raw))
}

func providerArgs(args ...string) []string {
	return append([]string{"provider"}, append(args, "--instance", "p0")...)
}

func randomPrivateKey(t *testing.T) solana.PrivateKey {
	t.Helper()

	privateKey, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)

	return privateKey
}

func receiveCall[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("daemon call did not progress")
	}

	var result T

	return result
}

func requireSubcommandNames(t *testing.T, parent *cobra.Command, names ...string) {
	t.Helper()

	for _, name := range names {
		findSubcommand(t, parent, name)
	}
}

func seedConfigAccount(t *testing.T, fakeRPC *sdktest.FakeRPC, chainCfg dvpn.Config) {
	t.Helper()
	require.NoError(t, fakeRPC.SetConfig(dvpn.ProgramID, chainCfg))
}

func seedGrantAccount(
	t *testing.T,
	fakeRPC *sdktest.FakeRPC,
	owner, operator solana.PublicKey,
	permissions uint64,
) {
	t.Helper()
	require.NoError(t, fakeRPC.SetGrant(dvpn.ProgramID, owner, operator, solana.PublicKey{}, permissions))
}

func seedProviderAccount(t *testing.T, fakeRPC *sdktest.FakeRPC, address solana.PublicKey, provider dvpn.Provider) {
	t.Helper()
	fakeRPC.SetAccount(
		address,
		sdktest.Account{Data: sdktest.EncodeAccount(dvpn.ProviderAccountDiscriminator, provider), Owner: dvpn.ProgramID},
	)
}

func startArgs(args ...string) []string {
	return clientArgs(append([]string{"start", "--node", "node-addr"}, args...)...)
}

func startFakeDaemon(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()

	endpoint := filepath.Join(t.TempDir(), "daemon.sock")

	var listenConfig net.ListenConfig

	ln, err := listenConfig.Listen(t.Context(), "unix", endpoint)
	require.NoError(t, err)

	srv := grpc.NewServer()

	register(srv)
	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(srv.Stop)

	return endpoint
}

func startFullFakeDaemon(t *testing.T) string {
	t.Helper()

	return startFakeDaemon(t, func(srv *grpc.Server) {
		daemonv1.RegisterDaemonServiceServer(srv, fakeDaemonService{})
		daemonv1.RegisterClientServiceServer(srv, fakeClientService{})
		daemonv1.RegisterNodeServiceServer(srv, fakeNodeService{})
		daemonv1.RegisterProviderServiceServer(srv, fakeProviderService{})
	})
}

func startNodeAPI(t *testing.T) (*httptest.Server, *fakeNodeAPI) {
	t.Helper()

	gin.SetMode(gin.TestMode)

	svc := &fakeNodeAPI{}
	options := api.EngineOptions{Logger: zerolog.Nop()}
	engine := api.NewEngine(options)
	nodeapi.RegisterRoutes(
		engine,
		svc,
		auth.NewNonceCache(time.Minute),
		api.NewLimiter(100, 100),
		api.NewLimiter(100, 100),
	)

	srv := httptest.NewTLSServer(engine)
	t.Cleanup(srv.Close)

	return srv, svc
}
