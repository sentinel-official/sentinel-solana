//go:build linux

package node

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/solana-foundation/solana-go/v2"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// heartbeatRenewWithin: how long before its listing lapses the node starts renewing it (capped at
// half the listing TTL). The worker checks every sendHeartbeatInterval from the cached chain state
// and sends nothing until then, so a renewal that fails (an RPC rate limit, say) is retried every
// few minutes for hours. Before, the node checked hourly and renewed only within the last hour, so
// one failed attempt let the listing lapse and every channel open failed with NodeNotLive for an
// hour or more.
const heartbeatRenewWithin = 6 * time.Hour

func renewWindow(ttlSeconds uint32) int64 {
	return min(int64(heartbeatRenewWithin.Seconds()), int64(ttlSeconds)/2)
}

func (r *Runtime) sendHeartbeat(ctx context.Context) error {
	chainCfg, node := r.chainState()

	remaining := node.ListedAt + int64(chainCfg.NodeListingTTLSeconds) - time.Now().Unix()
	if node.Status == dvpn.ListingStatusListed && remaining > renewWindow(chainCfg.NodeListingTTLSeconds) {
		return nil
	}

	if err := r.renewListing(ctx); err != nil {
		if node.Status == dvpn.ListingStatusListed && remaining <= 0 {
			return fmt.Errorf("node listing lapsed %d s ago; channel opens fail with NodeNotLive until it is renewed: %w",
				-remaining, err)
		}

		return err
	}

	return nil
}

func (r *Runtime) renewListing(ctx context.Context) error {

	grant, err := r.programClient.GrantAddress(ctx, r.owner, r.signer, dvpn.PermListNode, r.address)
	if err != nil {
		return err
	}

	instruction, err := dvpn.BuildListNodeInstruction(r.programClient.ProgramID, dvpn.ListNodeParams{
		Grant:  grant,
		Node:   r.address,
		Signer: r.signer,
	})
	if err != nil {
		return err
	}

	_, err = r.sender.Send(ctx, r.privateKey, []solana.Instruction{instruction}, tx.WithLabel("list_node"))
	if err != nil {
		return err
	}

	return r.syncChain(ctx)
}

func (r *Runtime) syncConfig(ctx context.Context) error {
	if err := r.syncChain(ctx); err != nil {
		return err
	}

	_, node := r.chainState()

	name, err := service.ParseName(r.cfg.Service)
	if err != nil {
		return err
	}

	var country [2]uint8
	copy(country[:], r.cfg.Country)
	want := dvpn.NodePrices{
		ChannelPricePerGB:   r.cfg.Prices.ChannelPerGB,
		ChannelPricePerHour: r.cfg.Prices.ChannelPerHour,
		LeasePricePerHour:   r.cfg.Prices.LeasePerHour,
	}
	got := dvpn.NodePrices{
		ChannelPricePerGB:   node.ChannelPricePerGB,
		ChannelPricePerHour: node.ChannelPricePerHour,
		LeasePricePerHour:   node.LeasePricePerHour,
	}
	uri := apiURL(r.cfg.API.Hosts[0], r.cfg.API.Port)
	uriBytes := []byte(uri)
	protocols := name.Bit()

	if r.cfg.PriceFeed.Enabled {
		want = got
	}

	if want == got && node.Protocols == protocols && node.Country == country &&
		string(node.URI[:node.URILen]) == uri {
		return nil
	}

	grant, err := r.programClient.GrantAddress(ctx, r.owner, r.signer, dvpn.PermUpdateNode, r.address)
	if err != nil {
		return err
	}

	instruction, err := dvpn.BuildUpdateNodeInstruction(r.programClient.ProgramID, dvpn.UpdateNodeParams{
		Country:   &country,
		Grant:     grant,
		Node:      r.address,
		Prices:    &want,
		Protocols: &protocols,
		Signer:    r.signer,
		URI:       &uriBytes,
	})
	if err != nil {
		return fmt.Errorf("building update_node instruction: %w", err)
	}

	_, err = r.sender.Send(ctx, r.privateKey, []solana.Instruction{instruction}, tx.WithLabel("update_node"))
	if err != nil {
		return err
	}

	return r.refreshMetadata(ctx)
}

func apiURL(host string, port uint16) string {
	return "https://" + net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))
}
