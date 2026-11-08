package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPC_StartSession_UnreachableChainIsUnavailableInPlainWords(t *testing.T) {
	t.Parallel()

	rt, home, _ := newWorkingRuntime(t)
	svc := startClientService(t, rt)
	req := newChannelRequest(home.NodeAddress.String())

	home.RPC.Server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := svc.StartSession(ctx, req)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code(), st.Message())
	require.Contains(t, st.Message(), "the network is unreachable or busy")
	require.NotContains(t, st.Message(), "rpc call")
	require.NotContains(t, st.Message(), home.RPC.Server.URL)
}
