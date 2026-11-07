//go:build linux

package node_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/node"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-go-sdk/v3/voucher"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

type failingRemoveServer struct {
	service.Server

	cancel context.CancelFunc
}

func (s failingRemoveServer) RemovePeer(context.Context, service.PeerID) error {
	s.cancel()

	return errors.New("peer removal failed")
}

// goneRemoveServer: the peer is already gone (OpenVPN client left / node restarted).
type goneRemoveServer struct {
	service.Server

	calls *int
}

func (s goneRemoveServer) RemovePeer(_ context.Context, id service.PeerID) error {
	*s.calls++

	return fmt.Errorf("%w: %s", service.ErrPeerNotFound, id)
}

type fakeCancelAfterAddPeer struct {
	service.Server

	cancel context.CancelFunc
}

func (s fakeCancelAfterAddPeer) AddPeer(ctx context.Context, peer service.PeerRequest) (service.PeerResponse, error) {
	response, err := s.Server.AddPeer(ctx, peer)
	if err == nil {
		s.cancel()
	}

	return response, err
}

func (s fakeCancelAfterAddPeer) RemovePeer(ctx context.Context, id service.PeerID) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("removing peer: %w", err)
	}

	if _, ok := ctx.Deadline(); !ok {
		return errors.New("peer removal has no deadline")
	}

	return s.Server.RemovePeer(ctx, id)
}

type fakeDelayedAddPeer struct {
	service.Server

	entered chan struct{}
	resume  chan struct{}
}

func (s *fakeDelayedAddPeer) AddPeer(ctx context.Context, peer service.PeerRequest) (service.PeerResponse, error) {
	close(s.entered)

	select {
	case <-ctx.Done():
		return service.PeerResponse{}, fmt.Errorf("waiting to add peer: %w", ctx.Err())
	case <-s.resume:
	}

	response, err := s.Server.AddPeer(ctx, peer)
	if err != nil {
		return service.PeerResponse{}, fmt.Errorf("adding delayed peer: %w", err)
	}

	return response, nil
}

type fakePinnedPeerID struct {
	service.Server

	active bool
}

func (s *fakePinnedPeerID) AddPeer(ctx context.Context, peer service.PeerRequest) (service.PeerResponse, error) {
	if s.active {
		return service.PeerResponse{}, service.ErrPeerExists
	}

	response, err := s.Server.AddPeer(ctx, peer)
	if err != nil {
		return service.PeerResponse{}, err
	}

	s.active = true
	response.ID = "pinned"

	return response, nil
}

func (s *fakePinnedPeerID) RemovePeer(ctx context.Context, id service.PeerID) error {
	if err := s.Server.RemovePeer(ctx, id); err != nil {
		return err
	}

	s.active = false

	return nil
}

type fakeRestoringServer struct {
	*sdktest.FakeServer

	restored []struct {
		req  service.PeerRequest
		resp service.PeerResponse
	}
}

func (s *fakeRestoringServer) RestorePeer(_ context.Context, req service.PeerRequest, resp service.PeerResponse) error {
	s.restored = append(s.restored, struct {
		req  service.PeerRequest
		resp service.PeerResponse
	}{
		req:  req,
		resp: resp,
	})

	return nil
}

func TestRuntime_CloseSession_CapsSettledAtPaid(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	_, chainNode := node.ChainState(r)
	terms := session.Terms{Basis: session.BasisPerGB, Price: chainNode.ChannelPricePerGB}
	block := session.Owed(terms.Basis, terms.Price, session.BytesPerGB)
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, voucherRequest(t, home, channel, terms, block))
	require.NoError(t, err)
	sess, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)
	state.SetStats(service.PeerID(sess.PeerID), service.PeerStats{RxBytes: session.BytesPerGB + session.BytesPerGB/2})

	require.NoError(t, node.CloseSession(t.Context(), r, sess, "test"))
	require.Equal(t, block, loadSettled(t, home, channel))
}

func TestRuntime_CloseSession_ConcurrentCopies(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	sess := seedSession(t, home, "session-concurrent", "peer-concurrent")
	frames, unsubscribe := node.Subscribe(r, sess.ID)
	t.Cleanup(unsubscribe)

	var workers sync.WaitGroup

	results := make(chan error, 2)

	for range 2 {
		current := *sess

		workers.Go(func() { results <- node.CloseSession(t.Context(), r, &current, "test") })
	}

	workers.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}

	decodeMeteringEvent(t, frames, nodeapi.EventClosed)
	require.Empty(t, frames)
	require.Len(t, state.RemovedPeers(), 1)
}

func TestRuntime_CloseSession_PublishesBeforePeerRemoval(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	sess := seedSession(t, home, "session-retry", "peer-retry")
	frames, unsubscribe := node.Subscribe(r, sess.ID)
	t.Cleanup(unsubscribe)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	node.SetService(r, failingRemoveServer{Server: sdktest.NewFakeServer(state), cancel: cancel})
	require.Error(t, node.CloseSession(ctx, r, sess, "credit exhausted"))
	closed := decodeMeteringEvent(t, frames, nodeapi.EventClosed)

	var event nodeapi.ClosedEvent
	require.NoError(t, json.Unmarshal(closed.Data, &event))
	require.Equal(t, nodeapi.ClosedReasonCreditExhausted, event.Reason)

	node.SetService(r, sdktest.NewFakeServer(state))
	require.NoError(t, node.SyncUsage(t.Context(), r))
	require.Empty(t, frames)
}

// A peer that is already gone is removed: the close finishes at once instead of retrying
// "peer not found": the close looped and never answered the client's DELETE.
func TestRuntime_CloseSession_PeerAlreadyGone(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	sess := seedSession(t, home, "session-gone", "peer-gone")
	calls := 0
	node.SetService(r, goneRemoveServer{Server: sdktest.NewFakeServer(state), calls: &calls})

	start := time.Now()
	require.NoError(t, node.CloseSession(t.Context(), r, sess, "closed by client"))
	require.Equal(t, 1, calls, "no retries for a peer that is gone")
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, node.SessionStatusClosed, sess.Status)
}

// countingFailRemoveServer: RemovePeer always fails (a transient node-side error), counting calls.
type countingFailRemoveServer struct {
	service.Server

	mu    *sync.Mutex
	calls *int
}

func (s countingFailRemoveServer) RemovePeer(context.Context, service.PeerID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	*s.calls++

	return errors.New("peer removal failed")
}

// The background retry ran 5 attempts with 2 s sleeps under the node-wide closeMu, so one
// stuck close blocked every open and close for ~30 s each cycle. It now makes one attempt per cycle.
func TestRuntime_RetryClosingSessions_OneAttemptPerCycle(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	sess := seedSession(t, home, "session-stuck", "peer-stuck")

	ctx, cancel := context.WithCancel(t.Context())
	node.SetService(r, failingRemoveServer{Server: sdktest.NewFakeServer(state), cancel: cancel})
	require.Error(t, node.CloseSession(ctx, r, sess, "closed by client"), "first close fails: session stays closing")

	var mu sync.Mutex

	calls := 0
	node.SetService(r, countingFailRemoveServer{Server: sdktest.NewFakeServer(state), mu: &mu, calls: &calls})

	start := time.Now()
	require.NoError(t, node.SyncUsage(t.Context(), r))
	require.Less(t, time.Since(start), time.Second, "no retry sleeps while holding closeMu")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, calls, "one attempt per sync cycle")
}

func TestRuntime_CloseSession_PublishesClosedEvent(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeStream)
	req := nodeapi.OpenSessionRequest{
		Credential: nodeapi.Credential{Channel: &channel, Type: nodeapi.CredentialTypeChannel},
	}
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	sess, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)

	frames, unsubscribe := node.Subscribe(r, sess.ID)
	defer unsubscribe()

	for range 8 {
		node.Publish(r, sess.ID, api.EncodeEvent(api.Event{Name: "voucher"}))
	}

	require.Len(t, frames, 8)
	require.NoError(t, node.CloseSession(t.Context(), r, sess, "closed by client"))
	require.Len(t, frames, 8)

	closedCount := 0

	for range 8 {
		select {
		case frame := <-frames:
			event, err := api.NewEventDecoder(bytes.NewReader(frame)).Next()
			require.NoError(t, err)

			if event.Name != nodeapi.EventClosed {
				continue
			}

			var closed nodeapi.ClosedEvent
			require.NoError(t, json.Unmarshal(event.Data, &closed))
			require.Equal(t, nodeapi.ClosedReasonClient, closed.Reason)

			closedCount++
		default:
			t.Fatal("missing queued event")
		}
	}

	require.Equal(t, 1, closedCount)
}

func TestRuntime_CloseSession_SettlesOnce(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	_, chainNode := node.ChainState(r)
	terms := session.Terms{Basis: session.BasisPerGB, Price: chainNode.ChannelPricePerGB}
	block := session.Owed(terms.Basis, terms.Price, session.BytesPerGB)
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, voucherRequest(t, home, channel, terms, block))
	require.NoError(t, err)
	sess, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)
	state.SetStats(service.PeerID(sess.PeerID), service.PeerStats{RxBytes: session.BytesPerGB / 2})

	require.NoError(t, node.CloseSession(t.Context(), r, sess, "test"))

	wantOwed := session.Owed(terms.Basis, terms.Price, session.BytesPerGB/2)
	require.Equal(t, wantOwed, loadSettled(t, home, channel))
	require.NotZero(t, sess.SettledUnix)
	require.Equal(t, node.SessionStatusClosed, sess.Status)

	sess.Status = node.SessionStatusClosing
	require.NoError(t, node.CloseSession(t.Context(), r, sess, "retry"))
	require.Equal(t, wantOwed, loadSettled(t, home, channel))
}

func TestRuntime_CloseSession_SkipsStreamLedger(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeStream)
	req := nodeapi.OpenSessionRequest{
		Credential: nodeapi.Credential{Channel: &channel, Type: nodeapi.CredentialTypeChannel},
	}
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	sess, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)

	sess.CreatedUnix -= 3600

	require.NoError(t, node.CloseSession(t.Context(), r, sess, "test"))

	require.Zero(t, loadSettled(t, home, channel))
	require.NotZero(t, sess.SettledUnix)
}

func TestRuntime_OpenSession_CancelledCreateRollsBackPeer(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeStream)
	req := nodeapi.OpenSessionRequest{
		Credential: nodeapi.Credential{
			Channel: &channel,
			Type:    nodeapi.CredentialTypeChannel,
		},
	}
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	node.SetService(r, fakeCancelAfterAddPeer{
		Server: sdktest.NewFakeServer(state),
		cancel: cancel,
	})
	_, _, err = node.OpenSession(ctx, r, admitted, peerRequestFixture())
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, state.AddedPeers(), 1)
	require.Equal(t, []service.PeerID{"a"}, state.RemovedPeers())
}

func TestRuntime_OpenSession_CloseAndReload(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	_, chainNode := node.ChainState(r)
	terms := session.Terms{Basis: session.BasisPerGB, Price: chainNode.ChannelPricePerGB}
	block := session.Owed(terms.Basis, terms.Price, session.BytesPerGB)
	req := voucherRequest(t, home, channel, terms, block)
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)

	peer := peerRequestFixture()
	sess, response, err := node.OpenSession(t.Context(), r, admitted, peer)
	require.NoError(t, err)
	require.Equal(t, admitted.Signer(), sess.Signer)
	require.Equal(t, string(response.ID), sess.PeerID)

	var storedPeer nodeapi.PeerRequest
	require.NoError(t, json.Unmarshal(sess.PeerRequest, &storedPeer))
	require.Equal(t, peer, storedPeer)
	require.True(t, node.HasEnforcer(r, sess.ID))
	require.NoError(t, node.CloseSession(t.Context(), r, sess, "test"))
	require.Equal(t, node.SessionStatusClosed, sess.Status)
	want := []service.PeerID{service.PeerID(sess.PeerID)}
	require.Equal(t, want, state.RemovedPeers())
	admitted2, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	sess2, _, err := node.OpenSession(t.Context(), r, admitted2, peer)
	require.NoError(t, err)
	runtime2, err := node.New(
		home.Loaded, home.DB, home.DaemonKey, home.ProgramClient, home.Sender, zerolog.Nop(), prometheus.NewRegistry(),
	)
	require.NoError(t, err)
	require.NoError(t, node.SyncChain(t.Context(), runtime2))
	node.SetService(runtime2, sdktest.NewFakeServer(state))
	require.NoError(t, node.ReloadSessions(t.Context(), runtime2))
	require.True(t, node.HasEnforcer(runtime2, sess2.ID))
	paid, ok := node.MeterPaid(runtime2, sess2.ID)
	require.True(t, ok)
	require.Equal(t, block, paid)
	require.Len(t, state.AddedPeers(), 3)
}

func TestRuntime_OpenSession_ConflictingPeerID(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	pinned := &fakePinnedPeerID{Server: sdktest.NewFakeServer(state)}
	node.SetService(r, pinned)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeStream)
	req := nodeapi.OpenSessionRequest{
		Credential: nodeapi.Credential{
			Channel: &channel,
			Type:    nodeapi.CredentialTypeChannel,
		},
	}
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	first, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)

	ownerB, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	channelB := seedChannel(t, r, home, ownerB, dvpn.PositionModeStream)
	req.Credential.Channel = &channelB
	admitted, err = node.Admit(t.Context(), r, ownerB.PublicKey(), req)
	require.NoError(t, err)
	_, _, err = node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.ErrorIs(t, err, service.ErrPeerExists)
	require.True(t, pinned.active)
	require.Empty(t, state.RemovedPeers())

	var stored node.Session
	require.NoError(t, home.DB.First(&stored, "id = ?", first.ID).Error)
	require.Equal(t, node.SessionStatusActive, stored.Status)
}

func TestRuntime_OpenSession_FailedAddPeerKeepsVoucher(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	_, chainNode := node.ChainState(r)
	terms := session.Terms{
		Basis: session.BasisPerGB,
		Price: chainNode.ChannelPricePerGB,
	}
	block := session.Owed(terms.Basis, terms.Price, session.BytesPerGB)
	seedPosition(t, r, home, channel, block)

	server := &fakePinnedPeerID{
		Server: sdktest.NewFakeServer(state),
		active: true,
	}
	node.SetService(r, server)
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, voucherRequest(t, home, channel, terms, 2*block))
	require.NoError(t, err)
	_, _, err = node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.ErrorIs(t, err, service.ErrPeerExists)

	var position node.Position
	require.NoError(t, home.DB.WithContext(t.Context()).First(&position, "address = ?", channel.String()).Error)
	require.Equal(t, block, position.Cumulative)
}

func TestRuntime_OpenSession_HonorsTopupDuringAddPeer(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	delayed := &fakeDelayedAddPeer{
		Server:  sdktest.NewFakeServer(state),
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
	}
	node.SetService(r, delayed)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeVoucher)
	_, chainNode := node.ChainState(r)
	terms := session.Terms{Basis: session.BasisPerGB, Price: chainNode.ChannelPricePerGB}
	block := session.Owed(terms.Basis, terms.Price, session.BytesPerGB)
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, voucherRequest(t, home, channel, terms, block))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	results := make(chan *node.Session, 1)
	errors := make(chan error, 1)

	go func() {
		sess, _, openErr := node.OpenSession(ctx, r, admitted, peerRequestFixture())
		results <- sess

		errors <- openErr
	}()

	select {
	case <-delayed.entered:
	case <-ctx.Done():
		t.Fatal("peer setup did not begin")
	}

	signed := voucher.Sign(home.OwnerKey, voucher.ChannelMessage(channel, channelOpenedUnix, 2*block))
	_, err = node.AcceptChannelVoucher(t.Context(), r, channel, home.OwnerAddress, nodeapi.Voucher{
		Cumulative: 2 * block,
		Signature:  signed.Signature[:],
	})
	require.NoError(t, err)
	close(delayed.resume)

	sess := <-results

	require.NoError(t, <-errors)

	paid, ok := node.MeterPaid(r, sess.ID)
	require.True(t, ok)
	require.Equal(t, 2*block, paid)
	state.SetStats(service.PeerID(sess.PeerID), service.PeerStats{RxBytes: session.BytesPerGB})
	require.NoError(t, node.SyncUsage(t.Context(), r))
	require.NoError(t, home.DB.WithContext(t.Context()).First(sess, "id = ?", sess.ID).Error)
	require.Equal(t, node.SessionStatusActive, sess.Status)
}

func TestRuntime_OpenSession_RefusesPeerIDOfClosingSession(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	pinned := &fakePinnedPeerID{Server: sdktest.NewFakeServer(state)}
	node.SetService(r, pinned)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeStream)
	req := nodeapi.OpenSessionRequest{
		Credential: nodeapi.Credential{
			Channel: &channel,
			Type:    nodeapi.CredentialTypeChannel,
		},
	}
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	first, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)
	require.NoError(t, home.DB.Model(&node.Session{}).Where("id = ?", first.ID).
		Update("status", node.SessionStatusClosing).Error)

	pinned.active = false
	admitted, err = node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	_, _, err = node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.ErrorContains(t, err, "creating session")

	var count int64
	require.NoError(t, home.DB.Model(&node.Session{}).Where("peer_id = ?", first.PeerID).Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestRuntime_OpenSession_ReusesClosedPeerID(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	pinned := &fakePinnedPeerID{Server: sdktest.NewFakeServer(state)}
	node.SetService(r, pinned)
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeStream)
	req := nodeapi.OpenSessionRequest{
		Credential: nodeapi.Credential{
			Channel: &channel,
			Type:    nodeapi.CredentialTypeChannel,
		},
	}
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	first, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)
	require.Equal(t, "pinned", first.PeerID)
	require.NoError(t, node.CloseSession(t.Context(), r, first, "test"))

	admitted, err = node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	second, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)
	require.Equal(t, "pinned", second.PeerID)
	require.NotEqual(t, first.ID, second.ID)
}

func TestRuntime_ReloadSessions_RestoresRecordedPeers(t *testing.T) {
	t.Parallel()

	r, home, state := newRuntime(t)
	node.SetService(r, &fakeRestoringServer{FakeServer: sdktest.NewFakeServer(state)})
	channel := seedChannel(t, r, home, home.OwnerKey, dvpn.PositionModeStream)
	req := nodeapi.OpenSessionRequest{
		Credential: nodeapi.Credential{
			Channel: &channel,
			Type:    nodeapi.CredentialTypeChannel,
		},
	}
	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	first, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)

	ownerB, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	channelB := seedChannel(t, r, home, ownerB, dvpn.PositionModeStream)
	req.Credential.Channel = &channelB
	admitted, err = node.Admit(t.Context(), r, ownerB.PublicKey(), req)
	require.NoError(t, err)

	peerB := peerRequestFixture()
	peerB.Payload = json.RawMessage(`{"public_key":"client-b"}`)
	second, responseB, err := node.OpenSession(t.Context(), r, admitted, peerB)
	require.NoError(t, err)
	require.NoError(t, node.CloseSession(t.Context(), r, first, "test"))

	ownerC, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	channelC := seedChannel(t, r, home, ownerC, dvpn.PositionModeStream)
	req.Credential.Channel = &channelC
	admitted, err = node.Admit(t.Context(), r, ownerC.PublicKey(), req)
	require.NoError(t, err)
	unrecorded, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)
	require.NoError(t, home.DB.Model(&node.Session{}).Where("id = ?", unrecorded.ID).
		Update("peer_response", nil).Error)

	runtime2, err := node.New(
		home.Loaded, home.DB, home.DaemonKey, home.ProgramClient, home.Sender, zerolog.Nop(), prometheus.NewRegistry(),
	)
	require.NoError(t, err)
	require.NoError(t, node.SyncChain(t.Context(), runtime2))

	restorer := &fakeRestoringServer{FakeServer: sdktest.NewFakeServer(state)}
	node.SetService(runtime2, restorer)

	addedBefore := len(state.AddedPeers())

	require.NoError(t, node.ReloadSessions(t.Context(), runtime2))
	require.Len(t, restorer.restored, 1)
	require.Equal(t, peerB, restorer.restored[0].req)
	require.Equal(t, responseB, restorer.restored[0].resp)
	require.Equal(t, service.PeerID(second.PeerID), restorer.restored[0].resp.ID)
	require.Len(t, state.AddedPeers(), addedBefore)

	var stored node.Session
	require.NoError(t, home.DB.First(&stored, "id = ?", unrecorded.ID).Error)
	require.Equal(t, node.SessionStatusClosed, stored.Status)
}
