//go:build linux

package node_test

import (
	"testing"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/node"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// A user with one ticket who disconnects and connects again at once must get in: the session the client
// just closed keeps its peer for the close reply, but it no longer holds the ticket's slot.
func TestRuntime_ClientClose_SameTicketReconnectsAtOnce(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	node.SetCloseGrace(r, time.Minute) // the old session stays closing for the whole test
	_, chainNode := node.ChainState(r)
	chainNode.LeasePricePerHour = 0
	node.SetChainNode(r, chainNode)

	provider := solana.PublicKey{71}
	home.RPC.SetAccount(provider, sdktest.Account{
		Data:  sdktest.EncodeAccount(dvpn.ProviderAccountDiscriminator, dvpn.Provider{Owner: home.OwnerAddress}),
		Owner: home.ProgramClient.ProgramID,
	})
	home.RPC.SetAccount(solana.PublicKey{72}, sdktest.Account{
		Data: sdktest.EncodeAccount(dvpn.LeaseAccountDiscriminator, dvpn.Lease{
			Node:         home.NodeAddress,
			NodeOpenedAt: chainNode.OpenedAt,
			OpenedAt:     time.Now().Unix() - 3600,
			Owner:        home.OwnerAddress,
			Provider:     provider,
			Status:       dvpn.PositionStatusOpen,
		}),
		Owner: home.ProgramClient.ProgramID,
	})
	encoded := auth.SignTicket(home.OwnerKey, auth.Ticket{
		DataCapBytes: 1 << 30,
		ExpiresUnix:  time.Now().Unix() + 3600,
		Node:         home.NodeAddress,
		Provider:     provider,
		User:         home.OwnerAddress,
		Version:      auth.TicketVersion,
	}).Encode()
	req := nodeapi.OpenSessionRequest{Credential: nodeapi.Credential{Ticket: &encoded, Type: nodeapi.CredentialTypeTicket}}

	admitted, err := node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err)
	first, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)

	_, err = node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.ErrorIs(t, err, api.ErrPeerLimitReached, "control: a live session holds the ticket's slot")

	require.NoError(t, node.CloseForClient(t.Context(), r, first))
	require.Equal(t, node.SessionStatusClosing, sessionStatus(t, home, first.ID), "the old peer is still there")

	start := time.Now()
	admitted, err = node.Admit(t.Context(), r, home.OwnerAddress, req)
	require.NoError(t, err, "the same ticket reconnects while the closed session keeps its peer")
	second, _, err := node.OpenSession(t.Context(), r, admitted, peerRequestFixture())
	require.NoError(t, err)
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.NotEqual(t, first.ID, second.ID)
	require.Equal(t, node.SessionStatusActive, sessionStatus(t, home, second.ID))
}
