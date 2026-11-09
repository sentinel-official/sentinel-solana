package providerapi

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
	"github.com/solana-foundation/solana-go/v2"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen -config cfg.yaml openapi.yaml

//nolint:gochecknoglobals
var spec = sync.OnceValue(func() *openapi3.T {
	s, err := GetSpec()
	if err != nil {
		panic(fmt.Errorf("loading providerapi spec: %w", err))
	}

	return s
})

// clientSpec is the spec a client checks responses against: it reads an error code it does not know
// (api.OpenErrorCodes), which ParseError keeps with its status and message.
//
//nolint:gochecknoglobals
var clientSpec = sync.OnceValue(func() *openapi3.T {
	s, err := GetSpec()
	if err != nil {
		panic(fmt.Errorf("loading providerapi client spec: %w", err))
	}

	api.OpenErrorCodes(s)

	return s
})

type Unimplemented struct{}

func (Unimplemented) GetHealth(context.Context, GetHealthRequestObject) (GetHealthResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetMetadata(context.Context, GetMetadataRequestObject) (GetMetadataResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetNodes(context.Context, GetNodesRequestObject) (GetNodesResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetPlanNodes(context.Context, GetPlanNodesRequestObject) (GetPlanNodesResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) IssueTicket(context.Context, IssueTicketRequestObject) (IssueTicketResponseObject, error) {
	return nil, api.ErrInternal
}

func Discover(
	ctx context.Context,
	programClient *program.Client,
	providerAddress solana.PublicKey,
	provider dvpn.Provider,
	timeout time.Duration,
	opts ...api.TransportOption,
) (Metadata, api.PeerURL, error) {
	uri, err := api.ParsePeerURL(program.ProviderURI(provider))
	if err != nil {
		return Metadata{}, api.PeerURL{}, fmt.Errorf("parsing provider uri: %w", err)
	}

	client, err := NewInsecureClient(uri.String(), timeout, opts...)
	if err != nil {
		return Metadata{}, api.PeerURL{}, fmt.Errorf("building insecure client: %w", err)
	}

	resp, err := client.GetMetadataWithResponse(ctx)
	if err != nil {
		return Metadata{}, api.PeerURL{}, fmt.Errorf("fetching provider metadata from %s: %w", uri, err)
	}

	if resp.JSON200 == nil {
		err := api.ParseError(resp.StatusCode(), resp.Body)

		return Metadata{}, api.PeerURL{}, fmt.Errorf("fetching provider metadata from %s: %w", uri, err)
	}

	metadata, err := VerifyMetadata(
		ctx,
		programClient,
		*resp.JSON200,
		providerAddress,
		provider.Owner,
		programClient.ProgramID,
		time.Now(),
	)
	if err != nil {
		return Metadata{}, api.PeerURL{}, fmt.Errorf("verifying provider metadata: %w", err)
	}

	if err := api.ValidateJSON(metadataSchema(), resp.JSON200.Payload); err != nil {
		return Metadata{}, api.PeerURL{}, fmt.Errorf("%w: metadata payload: %w", api.ErrInvalidResponse, err)
	}

	for i, raw := range metadata.API.URLs {
		if _, err := api.ParsePeerURL(raw); err != nil {
			return Metadata{}, api.PeerURL{}, fmt.Errorf("parsing api url %d: %w", i, err)
		}
	}

	return metadata, uri, nil
}

func NewInsecureClient(
	baseURL string,
	timeout time.Duration,
	opts ...api.TransportOption,
) (*ClientWithResponses, error) {
	insecure := api.NewInsecureHTTPClient(timeout, opts...)
	validated := api.NewValidatedTransport(insecure.Transport, clientSpec(), api.MaxResponseBodyBytes)
	insecure.Transport = &api.RetryTransport{Base: validated}

	return NewClientWithResponses(baseURL, WithHTTPClient(insecure))
}

func NewPinnedClient(
	baseURL,
	fingerprint string,
	timeout time.Duration,
	opts ...api.TransportOption,
) (*ClientWithResponses, error) {
	pinned := api.NewPinnedHTTPClient(fingerprint, timeout, opts...)
	validated := api.NewValidatedTransport(pinned.Transport, clientSpec(), api.MaxResponseBodyBytes)
	pinned.Transport = &api.RetryTransport{Base: validated}

	return NewClientWithResponses(baseURL, WithHTTPClient(pinned))
}

func RegisterRoutes(engine *gin.Engine, svc StrictServerInterface, byIP *api.Limiter) {
	onError := func(c *gin.Context, err error) { api.Abort(c, err) }
	onRequestError := func(c *gin.Context, err error) { api.AbortRequest(c, err) }
	options := StrictGinServerOptions{
		HandlerErrorFunc:         onError,
		RequestErrorHandlerFunc:  onRequestError,
		ResponseErrorHandlerFunc: onError,
	}
	middlewares := []StrictMiddlewareFunc{api.CopyContext[StrictHandlerFunc]}
	handler := NewStrictHandlerWithOptions(svc, middlewares, options)
	limitIP := MiddlewareFunc(api.LimitByIP(byIP))
	validate := MiddlewareFunc(api.ValidateRequests(spec()))
	ginMiddlewares := []MiddlewareFunc{limitIP, validate}
	ginOptions := GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, _ int) {
			api.AbortRequest(c, err)
		},
		Middlewares: ginMiddlewares,
	}

	RegisterHandlersWithOptions(engine, handler, ginOptions)
}

func SignMetadata(key solana.PrivateKey, metadata Metadata) (auth.SignedMessage, error) {
	signed, err := auth.SignMessage(key, auth.DomainProviderMetadata, metadata)
	if err != nil {
		return auth.SignedMessage{}, fmt.Errorf("signing metadata: %w", err)
	}

	return signed, nil
}

func VerifyMetadata(
	ctx context.Context,
	src auth.GrantSource,
	signed auth.SignedMessage,
	provider,
	owner,
	programID solana.PublicKey,
	now time.Time,
) (Metadata, error) {
	expected := auth.MetadataClaims{
		Operator:  signed.Signer,
		Owner:     owner,
		ProgramID: programID,
		Subject:   provider,
		Version:   auth.MetadataVersion,
	}
	check := auth.MetadataCheck{
		Domain:     auth.DomainProviderMetadata,
		Expected:   expected,
		Now:        now,
		Permission: program.PermSignProviderMetadata,
		TTL:        auth.MetadataTTL,
	}

	return auth.VerifyMetadata(ctx, src, signed, check, claims)
}

func claims(metadata Metadata) auth.MetadataClaims {
	return auth.MetadataClaims{
		ExpiresUnix: metadata.ExpiresAt,
		IssuedUnix:  metadata.IssuedAt,
		Operator:    metadata.Operator,
		Owner:       metadata.Owner,
		ProgramID:   metadata.ProgramID,
		Subject:     metadata.Provider,
		Version:     metadata.Version,
	}
}

func metadataSchema() *openapi3.Schema {
	return spec().Components.Schemas["Metadata"].Value
}
