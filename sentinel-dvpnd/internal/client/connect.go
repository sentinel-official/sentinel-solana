package client

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/solana-foundation/solana-go/v2"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

const requestTimeout = 15 * time.Second

var (
	errCredentialRequired = errors.New("credential is required")
	errMissingNodeAddrs   = errors.New("missing node addresses")
	errNodeNotAdmitted    = errors.New("node host is not admitted by the kill switch in place; StopSession releases it")
)

type cleanupStack []func() error

func (c *cleanupStack) push(fn func() error) { *c = append(*c, fn) }

func (c *cleanupStack) unwind() error {
	var err error
	for _, fn := range slices.Backward(*c) {
		err = errors.Join(err, fn())
	}

	return err
}

type dialedNode struct {
	client   *nodeapi.ClientWithResponses
	hosts    nodeHosts
	metadata nodeapi.Metadata
	stream   *nodeapi.ClientWithResponses
}

func (r *Runtime) activateSession(
	ctx context.Context,
	req *daemonv1.StartSessionRequest,
	conn *connection,
	opened *nodeapi.OpenSessionResponse,
	ticketHash string,
	placeholder *connection,
	cleanup *cleanupStack,
) error {
	now := time.Now()
	conn.lastContactUnix = now.Unix()
	conn.session.ID = opened.SessionID
	conn.session.NodeAddress = req.GetNode()
	conn.session.ProxyEndpoint = conn.service.ProxyEndpoint()
	conn.session.Status = SessionStatusActive
	conn.session.TicketHash = ticketHash

	conn.session.UpdatedUnix = now.Unix()
	if conn.channel != nil {
		conn.session.ChannelAddress = conn.channel.Address
	}

	if err := r.save(ctx, &conn.session); err != nil {
		return err
	}

	r.mu.Lock()
	if r.connection != placeholder {
		r.mu.Unlock()

		cleanupErr := cleanup.unwind()
		*cleanup = nil
		conn.session.EndedUnix = now.Unix()
		conn.session.Error = ErrStoppedDuringConnect.Error()
		conn.session.Status = SessionStatusEnded
		conn.session.UpdatedUnix = conn.session.EndedUnix
		r.saveEndedSession(ctx, &conn.session)

		return errors.Join(ErrStoppedDuringConnect, cleanupErr)
	}

	r.connection = conn
	r.publishLocked()
	r.mu.Unlock()
	r.metrics.sessionsActive.Inc()
	r.notify(&conn.session)

	return nil
}

func (r *Runtime) checkPublicAddrs(host string, addrs []netip.Addr) ([]netip.Addr, error) {
	if r.peerCfg.AllowPrivate {
		return addrs, nil
	}

	for _, addr := range addrs {
		if err := api.ValidatePublicAddr(addr); err != nil {
			return nil, fmt.Errorf("node %s: %w", host, err)
		}
	}

	return addrs, nil
}

func (r *Runtime) connect(
	ctx,
	lifecycle context.Context,
	req *daemonv1.StartSessionRequest,
	reconnecting bool,
	placeholder *connection,
) (_ *connection, err error) {
	nodeAddress, err := solana.PublicKeyFromBase58(req.GetNode())
	if err != nil {
		return nil, fmt.Errorf("parsing node address: %w", err)
	}

	nodeAccount, err := r.programClient.FetchNode(ctx, nodeAddress)
	if err != nil {
		return nil, fmt.Errorf("fetching node account %s: %w", nodeAddress, err)
	}

	node := nodeAccount.Value

	dial, err := r.dialNode(ctx, nodeAddress, node)
	if err != nil {
		return nil, err
	}

	terms := r.terms(node)
	conn := &connection{
		cancel:     placeholder.cancel,
		heartbeat:  max(time.Second, time.Duration(dial.metadata.Settlement.HeartbeatSeconds)*time.Second),
		nodeClient: dial.client,
		session: Session{
			NodeAddress: req.GetNode(),
			Service:     dial.metadata.Service.Name.String(),
		},
		streamClient: dial.stream,
	}

	body, requested, err := r.prepareCredential(
		ctx, req, conn, nodeAddress, node, terms, dial.metadata,
	)
	if err != nil {
		return nil, err
	}

	var cleanup cleanupStack

	defer func() {
		if err != nil {
			err = errors.Join(err, cleanup.unwind())

			if conn.session.ID != "" {
				conn.session.Clean = false
				conn.session.EndedUnix = time.Now().Unix()
				conn.session.Error = err.Error()
				conn.session.Status = SessionStatusEnded
				conn.session.UpdatedUnix = conn.session.EndedUnix
				r.saveEndedSession(ctx, &conn.session)
			}
		}
	}()

	opened, err := r.openRemoteSession(ctx, conn, body)
	if err != nil {
		if errors.Is(err, api.ErrChannelNodeStale) && !reconnecting {
			return r.reopenAndRetry(ctx, lifecycle, req, conn.channel, placeholder)
		}

		return nil, err
	}

	if opened.Peer.Name.String() != conn.session.Service {
		r.closeRemoteSession(ctx, conn.nodeClient, opened.SessionID)

		return nil, fmt.Errorf("%w: peer type %s", service.ErrInvalidPeer, opened.Peer.Name)
	}

	ticket, allowed, err := r.startTunnel(ctx, lifecycle, conn, opened, requested, dial.hosts, &cleanup)
	if err != nil {
		return nil, err
	}

	ticketHash, err := r.guardTunnel(ctx, conn, dial.hosts, allowed, ticket, placeholder, &cleanup)
	if err != nil {
		return nil, err
	}

	if err := r.activateSession(ctx, req, conn, opened, ticketHash, placeholder, &cleanup); err != nil {
		return nil, err
	}

	return conn, nil
}

func (r *Runtime) dialNode(ctx context.Context, address solana.PublicKey, node dvpn.Node) (dialedNode, error) {
	uri, err := api.ParsePeerURL(program.NodeURI(node))
	if err != nil {
		return dialedNode{}, fmt.Errorf("parsing node uri: %w", err)
	}

	addrs, err := r.resolveHost(ctx, uri.Host)
	if err != nil {
		return dialedNode{}, err
	}

	r.pin.SetHost(uri.Addr(), withPort(addrs, uri.Port))

	metadata, _, err := nodeapi.Discover(
		ctx, r.programClient, address, node, requestTimeout, api.WithDialContext(r.pin.DialContext),
	)
	if err != nil {
		return dialedNode{}, err
	}

	if metadata.Settlement.Prices != listedPrices(node) {
		return dialedNode{}, fmt.Errorf("checking node metadata prices: %w", session.ErrNodePriceChanged)
	}

	if err := service.ValidateMetadata(metadata.Service); err != nil {
		return dialedNode{}, fmt.Errorf("validating node service metadata: %w", err)
	}

	pinned, err := nodeapi.NewPinnedClient(
		uri.String(),
		metadata.API.TLSSHA256,
		r.privateKey,
		requestTimeout,
		api.WithDialContext(r.pin.DialContext),
	)
	if err != nil {
		return dialedNode{}, fmt.Errorf("building pinned client: %w", err)
	}

	stream, err := nodeapi.NewPinnedClient(
		uri.String(),
		metadata.API.TLSSHA256,
		r.privateKey,
		0,
		api.WithDialContext(r.pin.DialContext),
	)
	if err != nil {
		return dialedNode{}, fmt.Errorf("building pinned stream client: %w", err)
	}

	return dialedNode{
		client: pinned,
		hosts: nodeHosts{
			addrs: addrs,
			uri:   uri,
		},
		metadata: metadata,
		stream:   stream,
	}, nil
}

func (r *Runtime) guardTunnel(
	ctx context.Context,
	conn *connection,
	hosts nodeHosts,
	allowed allowlist,
	ticket *Ticket,
	placeholder *connection,
	cleanup *cleanupStack,
) (string, error) {
	if r.cfg.Network.KillSwitch != config.KillSwitchOff {
		prior, applied, err := r.applyFirewall(ctx, conn.iface, hosts, allowed, placeholder)
		if applied {
			cleanup.push(func() error { return r.rollbackFirewall(ctx, prior) })
		}

		if err != nil {
			return "", err
		}
	}

	if conn.iface != "" && r.cfg.Network.DNSBackend != config.DNSNone {
		if err := r.applyDNS(ctx, conn.iface, placeholder); err != nil {
			return "", err
		}

		cleanup.push(func() error { return r.revertDNS(ctx) })
	}

	if ticket != nil {
		if err := r.save(ctx, ticket); err != nil {
			return "", err
		}

		return ticket.Hash, nil
	}

	return "", nil
}

func (r *Runtime) openRemoteSession(
	ctx context.Context,
	conn *connection,
	body nodeapi.OpenSessionRequest,
) (*nodeapi.OpenSessionResponse, error) {
	name, err := service.ParseName(conn.session.Service)
	if err != nil {
		return nil, fmt.Errorf("parsing session service: %w", err)
	}

	conn.service, err = service.NewClient(name, r.services[name], r.placement(name), r.log)
	if err != nil {
		return nil, fmt.Errorf("constructing %s client: %w", name, err)
	}

	peerReq, err := conn.service.PeerRequest(ctx)
	if err != nil {
		return nil, fmt.Errorf("building peer request: %w", err)
	}

	body.Peer = peerReq

	var priorCumulative uint64
	if conn.payer != nil {
		priorCumulative = conn.payer.Paid()
	}

	conn.session.StartedUnix = time.Now().Unix()
	if body.Voucher != nil {
		conn.session.ID = uuid.NewString()
		conn.session.ChannelAddress = conn.channel.Address
		conn.session.Status = SessionStatusActive
		conn.session.PaidBaseUnits = max(priorCumulative, body.Voucher.Cumulative)
		conn.session.SettledBaseUnits = conn.nodeSettled
		conn.channel.Cumulative = conn.session.PaidBaseUnits

		conn.channel.UpdatedUnix = conn.session.StartedUnix
		if err := r.saveOpening(ctx, conn, ""); err != nil {
			conn.session.ID = ""

			return nil, err
		}
	}

	opened, err := r.postOpenSession(ctx, conn, body)
	if err != nil || body.Voucher == nil {
		return opened, err
	}

	view := nodeapi.Position{
		Consumption: opened.Consumption,
		Ledger:      opened.Ledger,
	}
	if err := r.verifyLedger(conn.channel, view, conn.terms, priorCumulative); err != nil {
		r.closeRemoteSession(ctx, conn.nodeClient, opened.SessionID)

		return nil, err
	}

	conn.payer.Resume(view.Ledger.CumulativeBaseUnits, view.Consumption.SettledBaseUnits)
	conn.payer.RecordIssued(body.Voucher.Cumulative)
	conn.nodeOwed = view.Consumption.OwedBaseUnits
	conn.nodeSettled = view.Consumption.SettledBaseUnits
	conn.nodeUsage = view.Consumption.Usage
	conn.session.PaidBaseUnits = conn.payer.Paid()
	conn.channel.Cumulative = conn.session.PaidBaseUnits
	conn.session.SettledBaseUnits = conn.nodeSettled
	conn.session.SpentBaseUnits = conn.nodeOwed - min(conn.nodeOwed, conn.nodeSettled)

	pendingID := conn.session.ID
	conn.session.ID = opened.SessionID

	pctx, cancel := r.withDetachedTimeout(ctx)
	defer cancel()

	if err := r.saveOpening(pctx, conn, pendingID); err != nil {
		conn.session.ID = pendingID
		r.closeRemoteSession(ctx, conn.nodeClient, opened.SessionID)

		return nil, err
	}

	return opened, nil
}

func (r *Runtime) postOpenSession(
	ctx context.Context,
	conn *connection,
	body nodeapi.OpenSessionRequest,
) (*nodeapi.OpenSessionResponse, error) {
	response, err := conn.nodeClient.OpenSessionWithResponse(ctx, body)
	if err != nil {
		return nil, fmt.Errorf("opening session: %w", err)
	}

	if response.JSON200 == nil {
		return nil, api.ParseError(response.StatusCode(), response.Body)
	}

	return response.JSON200, nil
}

func (r *Runtime) prepareCredential(
	ctx context.Context,
	req *daemonv1.StartSessionRequest,
	conn *connection,
	nodeAddress solana.PublicKey,
	node dvpn.Node,
	terms session.Terms,
	metadata nodeapi.Metadata,
) (nodeapi.OpenSessionRequest, *auth.TicketRequest, error) {
	var requested *auth.TicketRequest

	body := nodeapi.OpenSessionRequest{}

	switch kind := req.GetCredential().GetKind().(type) {
	case *daemonv1.Credential_Channel, *daemonv1.Credential_NewChannel:
		newChannel := req.GetCredential().GetNewChannel()
		if newChannel != nil && newChannel.GetMode() != uint32(dvpn.PositionModeStream) {
			block, err := r.blockFor(terms, metadata.Settlement)
			if err != nil {
				return nodeapi.OpenSessionRequest{}, nil, err
			}

			minBlock := metadata.Settlement.MinChannelPrepayBytes
			if terms.Basis == session.BasisPerHour {
				minBlock = metadata.Settlement.MinChannelPrepaySeconds
			}

			escrow := newChannel.GetEscrow()
			if escrow == 0 {
				escrow = r.cfg.EscrowBaseUnits
			}

			if err := checkNewChannelEscrow(escrow, terms, block, minBlock); err != nil {
				return nodeapi.OpenSessionRequest{}, nil, err
			}
		}

		ch, err := r.resolveChannel(ctx, req, nodeAddress, node)
		if err != nil {
			return nodeapi.OpenSessionRequest{}, nil, err
		}

		conn.channel = ch
		req.Credential = &daemonv1.Credential{Kind: &daemonv1.Credential_Channel{Channel: ch.Address}}
		body.Credential = nodeapi.Credential{
			Channel: publicKeyPtr(ch.Address),
			Type:    nodeapi.CredentialTypeChannel,
		}

		if ch.Mode == dvpn.PositionModeVoucher {
			block, err := r.blockFor(terms, metadata.Settlement)
			if err != nil {
				return nodeapi.OpenSessionRequest{}, nil, err
			}

			conn.block = block

			conn.minBlock = metadata.Settlement.MinChannelPrepayBytes
			if terms.Basis == session.BasisPerHour {
				conn.minBlock = metadata.Settlement.MinChannelPrepaySeconds
			}

			conn.terms = terms
			conn.payer = session.NewPayer(terms, block, ch.Escrow, r.cfg.TolerancePercent)
			conn.payer.Resume(ch.Cumulative, 0)

			view, err := r.syncLedger(ctx, conn)
			if err != nil {
				return nodeapi.OpenSessionRequest{}, nil, err
			}

			cumulative, err := conn.payer.Opening(view.Consumption.OwedBaseUnits, conn.minBlock)
			if errors.Is(err, session.ErrEscrowExhausted) {
				return nodeapi.OpenSessionRequest{}, nil, errInsufficientEscrow
			}

			if err != nil {
				return nodeapi.OpenSessionRequest{}, nil, err
			}

			v := r.signVoucher(ch, cumulative)
			body.Terms = &terms
			body.Voucher = &v
		}
	case *daemonv1.Credential_TicketRequest:
		signedRequest, err := r.signTicketRequest(kind.TicketRequest, nodeAddress)
		if err != nil {
			return nodeapi.OpenSessionRequest{}, nil, err
		}

		requested = &signedRequest.Request
		encoded := signedRequest.Encode()
		body.Credential = nodeapi.Credential{
			TicketRequest: &encoded,
			Type:          nodeapi.CredentialTypeTicketRequest,
		}
	case *daemonv1.Credential_TicketHash:
		var t Ticket
		if err := r.findFirst(ctx, &t, "hash = ?", kind.TicketHash); err != nil {
			return nodeapi.OpenSessionRequest{}, nil, fmt.Errorf("ticket %s: %w", kind.TicketHash, err)
		}

		body.Credential = nodeapi.Credential{
			Ticket: &t.Encoded,
			Type:   nodeapi.CredentialTypeTicket,
		}
	default:
		return nodeapi.OpenSessionRequest{}, nil, errCredentialRequired
	}

	return body, requested, nil
}

func (r *Runtime) reopenAndRetry(
	ctx,
	lifecycle context.Context,
	req *daemonv1.StartSessionRequest,
	stale *Channel,
	placeholder *connection,
) (*connection, error) {
	if stale != nil {
		r.log.Warn().Str("channel", stale.Address).Msg("node re-opened; closing stale channel and opening a new one")
		_ = r.startCloseChannel(ctx, stale)
	}

	mode := uint32(dvpn.PositionModeVoucher)
	if stale != nil {
		mode = uint32(stale.Mode)
	}

	newChannel := &daemonv1.NewChannel{
		Escrow: r.cfg.EscrowBaseUnits,
		Mode:   mode,
	}
	req.Credential = &daemonv1.Credential{Kind: &daemonv1.Credential_NewChannel{NewChannel: newChannel}}

	return r.connect(ctx, lifecycle, req, true, placeholder)
}

func (r *Runtime) resolveHost(ctx context.Context, host string) ([]netip.Addr, error) {
	r.mu.Lock()
	present, node := r.firewallState.present, r.nodeHosts
	r.mu.Unlock()

	if present && len(node.addrs) > 0 {
		if host == node.uri.Host {
			return r.checkPublicAddrs(host, node.addrs)
		}

		addr, err := netip.ParseAddr(host)
		if err != nil || !slices.Contains(node.addrs, addr.Unmap()) {
			return nil, fmt.Errorf("%w: %s", errNodeNotAdmitted, host)
		}

		return r.checkPublicAddrs(host, []netip.Addr{addr.Unmap()})
	}

	lctx, cancel := r.withTimeout(ctx)
	defer cancel()

	ips, err := r.lookup(lctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("resolving node %s: %w", host, err)
	}

	if len(ips) == 0 {
		return nil, fmt.Errorf("%w: node %s", errMissingNodeAddrs, host)
	}

	addrs := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		addrs = append(addrs, ip.Unmap().WithZone(""))
	}

	return r.checkPublicAddrs(host, mergeAddrs(addrs))
}

func (r *Runtime) saveOpening(ctx context.Context, conn *connection, pendingID string) error {
	if err := r.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if pendingID != "" {
			if err := db.Delete(&Session{}, "id = ?", pendingID).Error; err != nil {
				return fmt.Errorf("replacing pending opening exposure: %w", err)
			}
		}

		if err := db.Save(conn.channel).Error; err != nil {
			return fmt.Errorf("recording opening channel exposure: %w", err)
		}

		return db.Save(&conn.session).Error
	}); err != nil {
		return fmt.Errorf("saving opening exposure: %w", err)
	}

	return nil
}

func (r *Runtime) startTunnel(
	ctx,
	lifecycle context.Context,
	conn *connection,
	opened *nodeapi.OpenSessionResponse,
	requested *auth.TicketRequest,
	hosts nodeHosts,
	cleanup *cleanupStack,
) (*Ticket, allowlist, error) {
	cleanup.push(func() error {
		r.closeRemoteSession(ctx, conn.nodeClient, opened.SessionID)

		return nil
	})

	var (
		ticket *Ticket
		err    error
	)
	if opened.Ticket != nil {
		ticket, err = r.verifyTicket(ctx, *opened.Ticket, requested)
		if err != nil {
			return nil, allowlist{}, err
		}
	}

	endpoint, err := serviceEndpoint(hosts)
	if err != nil {
		return nil, allowlist{}, err
	}

	allowed, err := r.resolveAllowlist(ctx)
	if err != nil {
		return nil, allowlist{}, err
	}

	// A protocol that runs as a child process lives as long as the context it starts with. A stop cancels the
	// session context before teardown, which still takes the final usage sample and then stops the service
	// itself, so the service context follows the session context only while the service is starting.
	serviceCtx, cancelService := context.WithCancel(context.WithoutCancel(lifecycle))
	followSession := context.AfterFunc(lifecycle, cancelService)

	err = r.startService(serviceCtx, conn, endpoint.String(), opened.Peer)
	followSession()

	if err != nil {
		cancelService()

		return nil, allowlist{}, err
	}

	cleanup.push(func() error {
		r.stopService(ctx, conn.service)

		return nil
	})

	conn.iface = conn.service.Interface()

	return ticket, allowed, nil
}

func (r *Runtime) syncLedger(ctx context.Context, conn *connection) (nodeapi.Position, error) {
	address, _ := solana.PublicKeyFromBase58(conn.channel.Address)

	response, err := conn.nodeClient.GetChannelWithResponse(ctx, address)
	if err != nil {
		return nodeapi.Position{}, fmt.Errorf("reading node ledger: %w", err)
	}

	if response.JSON200 == nil {
		return nodeapi.Position{}, api.ParseError(response.StatusCode(), response.Body)
	}

	bound, err := r.settledBound(
		ctx,
		conn.channel.Address,
		session.Owed(conn.terms.Basis, conn.terms.Price, conn.block),
		r.cfg.TolerancePercent,
	)
	if err != nil {
		return nodeapi.Position{}, err
	}

	view := *response.JSON200
	if err := r.verifyLedger(conn.channel, view, conn.terms, bound); err != nil {
		return nodeapi.Position{}, err
	}

	conn.payer.Resume(view.Ledger.CumulativeBaseUnits, view.Consumption.SettledBaseUnits)
	conn.nodeOwed = view.Consumption.OwedBaseUnits
	conn.nodeSettled = view.Consumption.SettledBaseUnits
	conn.nodeUsage = view.Consumption.Usage

	return view, nil
}

func (r *Runtime) terms(node dvpn.Node) session.Terms {
	basis, _ := session.ParseBasis(r.cfg.Basis)
	if basis == session.BasisPerHour {
		return session.Terms{
			Basis: basis,
			Price: node.ChannelPricePerHour,
		}
	}

	return session.Terms{
		Basis: basis,
		Price: node.ChannelPricePerGB,
	}
}

func checkNewChannelEscrow(escrow uint64, terms session.Terms, block, minBlock uint64) error {
	if min(escrow, session.Owed(terms.Basis, terms.Price, block)) <
		session.Owed(terms.Basis, terms.Price, minBlock)/2 {
		return errInsufficientEscrow
	}

	return nil
}

func listedPrices(node dvpn.Node) nodeapi.Prices {
	return nodeapi.Prices{
		ChannelPricePerGB:   node.ChannelPricePerGB,
		ChannelPricePerHour: node.ChannelPricePerHour,
		LeasePricePerHour:   node.LeasePricePerHour,
	}
}

func publicKeyPtr(s string) *solana.PublicKey {
	k, _ := solana.PublicKeyFromBase58(s)

	return &k
}

func serviceEndpoint(hosts nodeHosts) (netip.Addr, error) {
	if len(hosts.addrs) == 0 {
		return netip.Addr{}, fmt.Errorf("%w: node %s", errMissingNodeAddrs, hosts.uri)
	}

	return hosts.addrs[0], nil
}
