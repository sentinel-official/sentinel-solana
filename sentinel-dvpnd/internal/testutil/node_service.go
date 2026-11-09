package testutil

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/solana-foundation/solana-go/v2"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-go-sdk/v3/voucher"
)

type FakeNodeOption func(*FakeNodeService)

type FakeNodeService struct {
	nodeapi.Unimplemented

	channel                        *fakePosition
	channelReads                   int
	channels                       map[solana.PublicKey]*fakePosition
	closedIDs                      []string
	currentSessionID               string
	getSessionCalls                int
	getSessionErr                  *api.Error
	getSessionErrRemaining         int
	heartbeatInterval              time.Duration
	leaseEventsJSON                bool
	leaseReads                     int
	leases                         map[solana.PublicKey]*fakePosition
	leaseVouchers                  []nodeapi.Voucher
	ledgerChannel                  solana.PublicKey
	metadata                       nodeapi.Metadata
	mu                             sync.Mutex
	closeGate                      chan struct{}
	closeEntered                   chan struct{}
	opened                         []nodeapi.OpenSessionRequest
	openRejections                 int
	peerName                       service.Name
	programClient                  *program.Client
	sendChannelVoucherErr          *api.Error
	sendChannelVoucherErrRemaining int
	sessionIDs                     []string
	sessions                       map[string]*fakeSession
	signer                         solana.PrivateKey
	streamOpens                    int
	subscribers                    map[*fakeSubscriber]solana.PublicKey
	ticket                         string
	vouchers                       []nodeapi.Voucher
}

func NewFakeNodeService(signer solana.PrivateKey, sessionID string, opts ...FakeNodeOption) *FakeNodeService {
	metadata := nodeapi.Metadata{Settlement: defaultFakeSettlement()}

	f := &FakeNodeService{
		channel:           newChannelPosition(metadata),
		channels:          make(map[solana.PublicKey]*fakePosition),
		currentSessionID:  sessionID,
		heartbeatInterval: time.Second,
		leases:            make(map[solana.PublicKey]*fakePosition),
		metadata:          metadata,
		peerName:          sdktest.FakeName,
		sessionIDs:        []string{sessionID},
		sessions:          map[string]*fakeSession{sessionID: {status: nodeapi.SessionStatusActive}},
		signer:            signer,
		subscribers:       make(map[*fakeSubscriber]solana.PublicKey),
	}

	for _, opt := range opts {
		opt(f)
	}

	return f
}

func (f *FakeNodeService) ChannelReadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.channelReads
}

func (f *FakeNodeService) ClosedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.closedIDs)
}

func (f *FakeNodeService) ClosedSessionID(index int) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.closedIDs[index]
}

func (f *FakeNodeService) CloseSession(
	ctx context.Context,
	req nodeapi.CloseSessionRequestObject,
) (nodeapi.CloseSessionResponseObject, error) {
	f.mu.Lock()
	gate, entered := f.closeGate, f.closeEntered
	f.closeEntered = nil
	f.mu.Unlock()

	if entered != nil {
		close(entered)
	}

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	sess, err := f.session(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if sess.status == nodeapi.SessionStatusActive {
		f.closedIDs = append(f.closedIDs, req.ID)
	}

	f.settleSession(sess, req.ID, nodeapi.ReasonClient)
	sess.status = nodeapi.SessionStatusClosed

	return nodeapi.CloseSession204Response{}, nil
}

func (f *FakeNodeService) GetChannel(
	ctx context.Context,
	req nodeapi.GetChannelRequestObject,
) (nodeapi.GetChannelResponseObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.channelReads++

	f.selectChannel(req.Channel)

	if err := f.syncChannel(ctx, req.Channel); err != nil {
		return nil, err
	}

	f.signLedger(req.Channel)

	return nodeapi.GetChannel200JSONResponse(f.channelView()), nil
}

func (f *FakeNodeService) GetLease(
	ctx context.Context,
	req nodeapi.GetLeaseRequestObject,
) (nodeapi.GetLeaseResponseObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.leaseReads++

	lease := f.lease(req.Lease)
	if err := f.syncLease(ctx, req.Lease, lease); err != nil {
		return nil, err
	}

	return nodeapi.GetLease200JSONResponse(lease.position()), nil
}

func (f *FakeNodeService) GetMetadata(
	context.Context,
	nodeapi.GetMetadataRequestObject,
) (nodeapi.GetMetadataResponseObject, error) {
	f.mu.Lock()
	metadata := f.metadata
	f.mu.Unlock()

	signed, err := nodeapi.SignMetadata(f.signer, metadata)
	if err != nil {
		return nil, fmt.Errorf("signing metadata: %w", err)
	}

	return nodeapi.GetMetadata200JSONResponse(signed), nil
}

func (f *FakeNodeService) GetSession(
	ctx context.Context,
	req nodeapi.GetSessionRequestObject,
) (nodeapi.GetSessionResponseObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.getSessionCalls++

	if f.getSessionErrRemaining > 0 && f.getSessionErr != nil {
		f.getSessionErrRemaining--

		return nodeapi.GetSessiondefaultJSONResponse{
			Body:       api.ErrorResponse{Error: *f.getSessionErr},
			StatusCode: f.getSessionErr.HTTPStatus,
		}, nil
	}

	sess, err := f.session(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	position := f.channelView()
	if sess.status != nodeapi.SessionStatusActive {
		position.Consumption = sess.consumption
	}

	return nodeapi.GetSession200JSONResponse{
		CloseReason: sess.closeReason,
		Consumption: position.Consumption,
		Ledger:      position.Ledger,
		Status:      sess.status,
	}, nil
}

func (f *FakeNodeService) GetSessionCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.getSessionCalls
}

func (f *FakeNodeService) LeaseReadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.leaseReads
}

func (f *FakeNodeService) LeaseVoucher(index int) nodeapi.Voucher {
	f.mu.Lock()
	defer f.mu.Unlock()

	return cloneVoucher(f.leaseVouchers[index])
}

func (f *FakeNodeService) LeaseVoucherCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.leaseVouchers)
}

func (f *FakeNodeService) OpenedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.opened)
}

func (f *FakeNodeService) OpenedRequest(index int) nodeapi.OpenSessionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	return cloneOpenRequest(f.opened[index])
}

func (f *FakeNodeService) OpenSession(
	ctx context.Context,
	req nodeapi.OpenSessionRequestObject,
) (nodeapi.OpenSessionResponseObject, error) {
	f.mu.Lock()

	f.opened = append(f.opened, cloneOpenRequest(*req.Body))

	if req.Body.Credential.Channel != nil {
		f.selectChannel(*req.Body.Credential.Channel)

		if err := f.syncChannel(ctx, *req.Body.Credential.Channel); err != nil {
			f.mu.Unlock()

			return nil, err
		}

		f.signLedger(*req.Body.Credential.Channel)
	}

	if f.openRejections > 0 {
		f.openRejections--
		f.mu.Unlock()

		return nodeapi.OpenSessiondefaultJSONResponse{
			Body:       api.ErrorResponse{Error: *api.ErrPrepayBelowMinimum},
			StatusCode: api.ErrPrepayBelowMinimum.HTTPStatus,
		}, nil
	}

	if err := f.validateOpening(ctx, *req.Body); err != nil {
		f.mu.Unlock()

		return nil, err
	}

	f.record(*req.Body)
	position := f.channelView()
	openedCount := len(f.opened)

	sessionID := f.sessionIDs[0]
	if openedCount > 1 {
		sessionID = uuid.NewString()
		f.sessionIDs = append(f.sessionIDs, sessionID)
	}

	f.currentSessionID = sessionID
	signer, _ := api.SignerFrom(ctx)
	f.sessions[sessionID] = &fakeSession{
		signer: signer,
		status: nodeapi.SessionStatusActive,
	}

	peerName := f.peerName
	ticket := f.ticket
	f.mu.Unlock()

	resp := nodeapi.OpenSession200JSONResponse{
		Consumption: position.Consumption,
		Ledger:      position.Ledger,
		Peer: service.PeerResponse{
			ID:      "srv",
			Name:    peerName,
			Payload: []byte(`{"addresses":["10.8.0.2/32"]}`),
		},
		SessionID: sessionID,
	}

	if ticket != "" {
		resp.Ticket = &ticket
	}

	return resp, nil
}

func (f *FakeNodeService) ResumeHeartbeats(interval time.Duration) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.heartbeatInterval = interval

	return f.streamOpens + 1
}

func (f *FakeNodeService) SeedLease(
	signer solana.PrivateKey,
	address solana.PublicKey,
	cumulative, settled, escrow, ratePerHour uint64,
	openedUnix int64,
) {
	f.mu.Lock()
	defer f.mu.Unlock()

	signed := voucher.Sign(signer, voucher.LeaseMessage(address, openedUnix, cumulative))
	f.leases[address] = &fakePosition{
		ledger: nodeapi.Ledger{
			Basis:               session.BasisPerHour,
			CumulativeBaseUnits: cumulative,
			EscrowBaseUnits:     escrow,
			OpenedAt:            openedUnix,
			Price:               ratePerHour,
			Signature:           signed.Signature[:],
			Status:              nodeapi.LedgerStatusOpen,
		},
		settled: settled,
		signer:  slices.Clone(signer),
	}
}

func (f *FakeNodeService) SeedLedger(signer solana.PrivateKey, cumulative, settled uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.channel.ledger.CumulativeBaseUnits = cumulative
	f.channel.ledger.Signature = nil
	f.channel.settled = settled
	f.channel.signer = slices.Clone(signer)
	f.signLedger(f.ledgerChannel)
}

func (f *FakeNodeService) SendChannelVoucher(
	ctx context.Context,
	req nodeapi.SendChannelVoucherRequestObject,
) (nodeapi.SendChannelVoucherResponseObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.sendChannelVoucherErrRemaining > 0 && f.sendChannelVoucherErr != nil {
		f.sendChannelVoucherErrRemaining--

		return nodeapi.SendChannelVoucherdefaultJSONResponse{
			Body:       api.ErrorResponse{Error: *f.sendChannelVoucherErr},
			StatusCode: f.sendChannelVoucherErr.HTTPStatus,
		}, nil
	}

	f.selectChannel(req.Channel)

	if req.Body != nil {
		if err := f.validateVoucher(ctx, f.channel, req.Channel, req.Body.Voucher, false); err != nil {
			return nil, err
		}

		f.vouchers = append(f.vouchers, cloneVoucher(req.Body.Voucher))
		f.channel.accept(req.Body.Voucher)
	}

	return nodeapi.SendChannelVoucher200JSONResponse(f.channelView()), nil
}

func (f *FakeNodeService) SendLeaseVoucher(
	ctx context.Context,
	req nodeapi.SendLeaseVoucherRequestObject,
) (nodeapi.SendLeaseVoucherResponseObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	lease := f.lease(req.Lease)
	if req.Body != nil {
		if err := f.validateVoucher(ctx, lease, req.Lease, req.Body.Voucher, true); err != nil {
			return nil, err
		}

		f.leaseVouchers = append(f.leaseVouchers, cloneVoucher(req.Body.Voucher))
		lease.accept(req.Body.Voucher)
	}

	return nodeapi.SendLeaseVoucher200JSONResponse(lease.position()), nil
}

func (f *FakeNodeService) SessionIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.sessionIDs)
}

func (f *FakeNodeService) SetEscrow(escrow uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.channel.ledger.EscrowBaseUnits = escrow
}

func (f *FakeNodeService) SetGetSessionErr(err *api.Error, remaining int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.getSessionErr = err
	f.getSessionErrRemaining = remaining
}

func (f *FakeNodeService) SetHeartbeatInterval(interval time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.heartbeatInterval = interval
}

func (f *FakeNodeService) SetLeaseEventsJSON(enabled bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.leaseEventsJSON = enabled
}

func (f *FakeNodeService) SetLeaseStatus(address solana.PublicKey, status nodeapi.LedgerStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.lease(address).ledger.Status = status
}

func (f *FakeNodeService) SetLeaseUsage(address solana.PublicKey, usage session.Usage) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.lease(address).usage = usage
}

func (f *FakeNodeService) SetMetadata(metadata nodeapi.Metadata) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.metadata = metadata
	f.setChannelPrice()
}

func (f *FakeNodeService) SetOpenRejections(count int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.openRejections = count
}

func (f *FakeNodeService) SetPeerName(name service.Name) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.peerName = name
}

func (f *FakeNodeService) SetRedeemed(redeemed uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.channel.ledger.RedeemedBaseUnits = redeemed
}

func (f *FakeNodeService) SetSendChannelVoucherErr(err *api.Error, remaining int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sendChannelVoucherErr = err
	f.sendChannelVoucherErrRemaining = remaining
}

func (f *FakeNodeService) SetSessionSigner(signer solana.PublicKey) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sessions[f.currentSessionID].signer = signer
}

func (f *FakeNodeService) SetSessionStatus(status nodeapi.SessionStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sessions[f.currentSessionID].status = status
}

func (f *FakeNodeService) SetSettled(settled uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.channel.settled = settled
}

func (f *FakeNodeService) SetTicket(encoded string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ticket = encoded
}

func (f *FakeNodeService) SetUsage(usage session.Usage) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.channel.usage = usage
}

func (f *FakeNodeService) StreamOpenCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.streamOpens
}

func (f *FakeNodeService) Voucher(index int) nodeapi.Voucher {
	f.mu.Lock()
	defer f.mu.Unlock()

	return cloneVoucher(f.vouchers[index])
}

func (f *FakeNodeService) VoucherCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.vouchers)
}

func (f *FakeNodeService) channelView() nodeapi.Position {
	view := f.channel.position()
	if sess := f.sessions[f.currentSessionID]; sess != nil && sess.status != nodeapi.SessionStatusActive {
		view.Consumption = nodeapi.Consumption{
			OwedBaseUnits:    f.channel.settled,
			SettledBaseUnits: f.channel.settled,
		}
	}

	return view
}

func (f *FakeNodeService) lease(address solana.PublicKey) *fakePosition {
	lease, ok := f.leases[address]
	if !ok {
		lease = &fakePosition{ledger: nodeapi.Ledger{
			Basis:           session.BasisPerHour,
			EscrowBaseUnits: 1_000_000_000,
			Price:           f.metadata.Settlement.Prices.LeasePricePerHour,
			Status:          nodeapi.LedgerStatusOpen,
		}}
		f.leases[address] = lease
	}

	return lease
}

func (f *FakeNodeService) publishClosed(sessionID, reason string) {
	for subscriber := range f.subscribers {
		if subscriber.sessionID == sessionID {
			subscriber.enqueue(closedFrame(reason), true)
		}
	}
}

func (f *FakeNodeService) record(req nodeapi.OpenSessionRequest) {
	if req.Terms != nil {
		f.channel.ledger.Basis = req.Terms.Basis
		f.setChannelPrice()
	}

	if req.Voucher != nil && req.Credential.Channel != nil {
		f.channel.accept(*req.Voucher)
	}
}

func (f *FakeNodeService) selectChannel(address solana.PublicKey) {
	if address.IsZero() || address == f.ledgerChannel {
		return
	}

	if f.ledgerChannel.IsZero() {
		f.channels[address] = f.channel
		f.ledgerChannel = address

		return
	}

	position, ok := f.channels[address]
	if !ok {
		position = newChannelPosition(f.metadata)
		f.channels[address] = position
	}

	f.channel = position
	f.ledgerChannel = address
}

func (f *FakeNodeService) session(ctx context.Context, id string) (*fakeSession, error) {
	signer, signed := api.SignerFrom(ctx)
	if id == "" && !signed {
		id = f.currentSessionID
	}

	sess := f.sessions[id]
	if sess == nil {
		return nil, api.ErrSessionNotFound
	}

	if signed && !sess.signer.IsZero() && sess.signer != signer {
		return nil, api.ErrUnauthorized
	}

	return sess, nil
}

func (f *FakeNodeService) setChannelPrice() {
	f.channel.ledger.Price = f.metadata.Settlement.Prices.ChannelPricePerGB
	if f.channel.ledger.Basis == session.BasisPerHour {
		f.channel.ledger.Price = f.metadata.Settlement.Prices.ChannelPricePerHour
	}
}

func (f *FakeNodeService) settleSession(sess *fakeSession, id, reason string) {
	if sess.status != nodeapi.SessionStatusActive {
		return
	}

	view := f.channel.position()
	f.channel.settled = min(view.Consumption.OwedBaseUnits, f.channel.ledger.CumulativeBaseUnits)
	sess.consumption = nodeapi.Consumption{
		OwedBaseUnits:    f.channel.settled,
		SettledBaseUnits: f.channel.settled,
	}
	sess.closeReason = nodeapi.ClosedReason(reason)
	sess.status = nodeapi.SessionStatusClosing

	f.publishClosed(id, reason)
}

func (f *FakeNodeService) signLedger(channel solana.PublicKey) {
	if channel.IsZero() || f.channel.signer == nil || f.channel.ledger.Signature != nil {
		return
	}

	message := voucher.ChannelMessage(channel, f.channel.ledger.OpenedAt, f.channel.ledger.CumulativeBaseUnits)
	signed := voucher.Sign(f.channel.signer, message)
	f.channel.ledger.Signature = signed.Signature[:]
}

func (f *FakeNodeService) syncChannel(ctx context.Context, address solana.PublicKey) error {
	if f.programClient == nil {
		return nil
	}

	account, err := f.programClient.FetchChannel(ctx, address)
	if err != nil {
		return fmt.Errorf("fetching channel %s: %w", address, err)
	}

	f.channel.ledger.EscrowBaseUnits = account.Value.Escrow
	f.channel.ledger.OpenedAt = account.Value.OpenedAt
	f.channel.ledger.RedeemedBaseUnits = account.Value.Redeemed

	return nil
}

func (f *FakeNodeService) syncLease(ctx context.Context, address solana.PublicKey, lease *fakePosition) error {
	if f.programClient == nil {
		return nil
	}

	account, err := f.programClient.FetchLease(ctx, address)
	if err != nil {
		return fmt.Errorf("fetching lease %s: %w", address, err)
	}

	lease.ledger.EscrowBaseUnits = account.Value.Escrow
	lease.ledger.OpenedAt = account.Value.OpenedAt
	lease.ledger.RedeemedBaseUnits = account.Value.Redeemed

	return nil
}

func (f *FakeNodeService) validateOpening(ctx context.Context, req nodeapi.OpenSessionRequest) error {
	if _, signed := api.SignerFrom(ctx); !signed || req.Credential.Channel == nil {
		return nil
	}

	if req.Terms == nil || req.Voucher == nil || req.Terms.Validate() != nil {
		return api.ErrTermsRejected
	}

	price := f.metadata.Settlement.Prices.ChannelPricePerGB

	minimum := f.metadata.Settlement.MinChannelPrepayBytes
	if req.Terms.Basis == session.BasisPerHour {
		price = f.metadata.Settlement.Prices.ChannelPricePerHour
		minimum = f.metadata.Settlement.MinChannelPrepaySeconds
	}

	if req.Terms.Price != price {
		return api.ErrTermsRejected
	}

	if err := f.validateVoucher(ctx, f.channel, *req.Credential.Channel, *req.Voucher, false); err != nil {
		return err
	}

	paid := max(f.channel.ledger.CumulativeBaseUnits, req.Voucher.Cumulative)
	if paid < f.channel.settled || session.Headroom(paid, f.channel.settled) <
		session.Owed(req.Terms.Basis, price, minimum)/2 {
		return api.ErrPrepayBelowMinimum
	}

	return nil
}

func (f *FakeNodeService) validateVoucher(
	ctx context.Context,
	position *fakePosition,
	address solana.PublicKey,
	submitted nodeapi.Voucher,
	lease bool,
) error {
	signer, signed := api.SignerFrom(ctx)
	if !signed {
		return nil
	}

	message := voucher.ChannelMessage(address, position.ledger.OpenedAt, submitted.Cumulative)
	if lease {
		message = voucher.LeaseMessage(address, position.ledger.OpenedAt, submitted.Cumulative)
	}

	if voucher.Verify(signer, message, submitted.Signature) != nil {
		return api.ErrInvalidVoucherSignature
	}

	if submitted.Cumulative > position.ledger.EscrowBaseUnits {
		return api.ErrVoucherExceedsEscrow
	}

	return nil
}

type fakePosition struct {
	ledger  nodeapi.Ledger
	settled uint64
	signer  solana.PrivateKey
	usage   session.Usage
}

func (p *fakePosition) accept(submitted nodeapi.Voucher) {
	if submitted.Cumulative > p.ledger.CumulativeBaseUnits {
		p.ledger.CumulativeBaseUnits = submitted.Cumulative
		p.ledger.Signature = slices.Clone(submitted.Signature)
	}
}

func (p *fakePosition) position() nodeapi.Position {
	owed := session.Owed(p.ledger.Basis, p.ledger.Price, p.usage.Amount(p.ledger.Basis))
	owed += min(p.settled, math.MaxUint64-owed)
	ledger := p.ledger
	ledger.Signature = slices.Clone(ledger.Signature)

	return nodeapi.Position{
		Consumption: nodeapi.Consumption{
			OwedBaseUnits:    owed,
			SettledBaseUnits: p.settled,
			Usage:            p.usage,
		},
		Ledger: ledger,
	}
}

type fakeSession struct {
	closeReason nodeapi.ClosedReason
	consumption nodeapi.Consumption
	signer      solana.PublicKey
	status      nodeapi.SessionStatus
}

func WithProgramClient(programClient *program.Client) FakeNodeOption {
	return func(f *FakeNodeService) { f.programClient = programClient }
}

func cloneOpenRequest(req nodeapi.OpenSessionRequest) nodeapi.OpenSessionRequest {
	req.Peer.Payload = slices.Clone(req.Peer.Payload)
	if req.Credential.Channel != nil {
		channel := *req.Credential.Channel
		req.Credential.Channel = &channel
	}

	if req.Credential.Ticket != nil {
		ticket := *req.Credential.Ticket
		req.Credential.Ticket = &ticket
	}

	if req.Credential.TicketRequest != nil {
		ticketRequest := *req.Credential.TicketRequest
		req.Credential.TicketRequest = &ticketRequest
	}

	if req.Terms != nil {
		terms := *req.Terms
		req.Terms = &terms
	}

	if req.Voucher != nil {
		submitted := cloneVoucher(*req.Voucher)
		req.Voucher = &submitted
	}

	return req
}

func cloneVoucher(submitted nodeapi.Voucher) nodeapi.Voucher {
	submitted.Signature = slices.Clone(submitted.Signature)

	return submitted
}

func defaultFakeSettlement() nodeapi.Settlement {
	return nodeapi.Settlement{
		HeartbeatSeconds:        1,
		MinChannelPrepayBytes:   100_000_000,
		MinChannelPrepaySeconds: 60,
		MinLeasePrepaySeconds:   3600,
		Prices: nodeapi.Prices{
			ChannelPricePerGB:   5_000,
			ChannelPricePerHour: 200_000,
			LeasePricePerHour:   200_000,
		},
	}
}

func newChannelPosition(metadata nodeapi.Metadata) *fakePosition {
	return &fakePosition{ledger: nodeapi.Ledger{
		Basis:           session.BasisPerGB,
		EscrowBaseUnits: 1_000_000_000,
		Price:           metadata.Settlement.Prices.ChannelPricePerGB,
		Status:          nodeapi.LedgerStatusOpen,
	}}
}

// HoldClose makes CloseSession wait until release is called. entered is closed when the first held close arrives.
func (f *FakeNodeService) HoldClose() (entered <-chan struct{}, release func()) {
	gate, in := make(chan struct{}), make(chan struct{})

	f.mu.Lock()
	f.closeGate, f.closeEntered = gate, in
	f.mu.Unlock()

	var once sync.Once

	return in, func() { once.Do(func() { close(gate) }) }
}
