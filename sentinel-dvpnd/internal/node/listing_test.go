//go:build linux

package node_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/node"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

func TestRuntime_SendHeartbeat_SendsWhenListingStale(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	chainCfg, chainNode := node.ChainState(r)
	chainNode.ListedAt = time.Now().Unix() - int64(chainCfg.NodeListingTTLSeconds)
	node.SetChainNode(r, chainNode)
	require.NoError(t, node.SendHeartbeat(t.Context(), r))
	require.Len(t, home.RPC.Sent(), 1)
}

// A node renews its listing hours before it lapses, not only in its last hour: a renewal that fails
// (the RPC rate limits the node, say) is then retried many times before channel opens see
// NodeNotLive.
func TestRuntime_SendHeartbeat_RenewsHoursBeforeTheListingLapses(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	chainCfg, chainNode := node.ChainState(r)
	require.GreaterOrEqual(t, chainCfg.NodeListingTTLSeconds, uint32(12*3600), "fixture TTL")

	chainNode.ListedAt = time.Now().Unix() - int64(chainCfg.NodeListingTTLSeconds) + 3*3600
	node.SetChainNode(r, chainNode)
	require.NoError(t, node.SendHeartbeat(t.Context(), r))
	require.Len(t, home.RPC.Sent(), 1, "3 h left: renew now, leaving hours for retries")
}

func TestRuntime_SendHeartbeat_LeavesAFreshListingAlone(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	_, chainNode := node.ChainState(r)
	chainNode.ListedAt = time.Now().Unix()
	node.SetChainNode(r, chainNode)
	require.NoError(t, node.SendHeartbeat(t.Context(), r))
	require.Empty(t, home.RPC.Sent())
}

func TestRuntime_SendHeartbeat_RetriesWithinMinutes(t *testing.T) {
	t.Parallel()

	require.LessOrEqual(t, node.SendHeartbeatInterval, 10*time.Minute,
		"a failed renewal waits one worker interval before the next try")
}

func TestRuntime_SyncConfig_RefreshesCacheAfterPush(t *testing.T) {
	t.Parallel()

	r, home, _ := newRuntime(t)
	prices := dvpn.NodePrices{
		ChannelPricePerGB:   11,
		ChannelPricePerHour: 22,
		LeasePricePerHour:   33,
	}
	home.Loaded.Node.Prices.ChannelPerGB = prices.ChannelPricePerGB
	home.Loaded.Node.Prices.ChannelPerHour = prices.ChannelPricePerHour
	home.Loaded.Node.Prices.LeasePerHour = prices.LeasePricePerHour

	_, chainNode := node.ChainState(r)
	oldPrices := dvpn.NodePrices{
		ChannelPricePerGB:   chainNode.ChannelPricePerGB,
		ChannelPricePerHour: chainNode.ChannelPricePerHour,
		LeasePricePerHour:   chainNode.LeasePricePerHour,
	}

	require.NoError(t, node.RefreshMetadata(t.Context(), r))
	response, err := node.NewHandler(r).GetMetadata(t.Context(), nodeapi.GetMetadataRequestObject{})
	require.NoError(t, err)

	var metadata nodeapi.Metadata
	require.NoError(t, nodeapi.SignedMessage(response.(nodeapi.GetMetadata200JSONResponse)).Decode(&metadata))
	require.Equal(t, nodeapi.Prices{
		ChannelPricePerGB:   oldPrices.ChannelPricePerGB,
		ChannelPricePerHour: oldPrices.ChannelPricePerHour,
		LeasePricePerHour:   oldPrices.LeasePricePerHour,
	}, metadata.Settlement.Prices)

	chainNode.ChannelPricePerGB = prices.ChannelPricePerGB
	chainNode.ChannelPricePerHour = prices.ChannelPricePerHour
	chainNode.LeasePricePerHour = prices.LeasePricePerHour
	home.RPC.Server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("reading RPC request: %v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)

			return
		}

		req.Body = io.NopCloser(bytes.NewReader(body))

		var rpcReq struct {
			Method string `json:"method"`
		}
		if err := json.Unmarshal(body, &rpcReq); err != nil {
			t.Errorf("decoding RPC request: %v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)

			return
		}

		home.RPC.ServeHTTP(w, req)

		if rpcReq.Method == "sendTransaction" {
			home.RPC.SetNode(home.ProgramClient.ProgramID, home.NodeAddress, chainNode)
		}
	})

	require.NoError(t, node.SyncConfig(t.Context(), r))
	require.Len(t, home.RPC.Sent(), 1)

	_, got := node.ChainState(r)
	gotPrices := dvpn.NodePrices{
		ChannelPricePerGB:   got.ChannelPricePerGB,
		ChannelPricePerHour: got.ChannelPricePerHour,
		LeasePricePerHour:   got.LeasePricePerHour,
	}
	require.Equal(t, prices, gotPrices)
	response, err = node.NewHandler(r).GetMetadata(t.Context(), nodeapi.GetMetadataRequestObject{})
	require.NoError(t, err)
	require.NoError(t, nodeapi.SignedMessage(response.(nodeapi.GetMetadata200JSONResponse)).Decode(&metadata))
	require.Equal(t, nodeapi.Prices{
		ChannelPricePerGB:   prices.ChannelPricePerGB,
		ChannelPricePerHour: prices.ChannelPricePerHour,
		LeasePricePerHour:   prices.LeasePricePerHour,
	}, metadata.Settlement.Prices)
}
