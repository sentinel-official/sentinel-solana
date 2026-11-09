package nodeapi_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
)

// TestBusyAnswer_PassesTheClientValidator runs the node's chain-busy 429 through the client's
// response validator with the node spec, which requires Retry-After on every 429.
func TestBusyAnswer_PassesTheClientValidator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Parallel()

	engine := gin.New()
	engine.GET("/v1/channels/:channel", func(ctx *gin.Context) {
		api.Abort(ctx, fmt.Errorf("fetching channel: %w", api.ErrChainBusy))
	})

	srv := httptest.NewServer(engine)
	t.Cleanup(srv.Close)

	spec, err := nodeapi.GetSwagger()
	require.NoError(t, err)

	client := &http.Client{Transport: api.NewValidatedTransport(http.DefaultTransport, spec, api.MaxResponseBodyBytes)}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		srv.URL+"/v1/channels/"+solana.PublicKey{7}.String(), nil)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err, "the client must accept the busy answer, not reject it as invalid")

	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	require.NotEmpty(t, resp.Header.Get("Retry-After"))
}
