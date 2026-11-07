package nodeapi

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
	"github.com/jellydator/ttlcache/v3"
	"github.com/solana-foundation/solana-go/v2"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen -config cfg.yaml openapi.yaml

const (
	EventClosed           = "closed"
	EventVoucherRequest   = "voucher_request"
	ReasonClient          = "closed by client"
	ReasonCloseGrace      = "close grace elapsed"
	ReasonCreditExhausted = "credit exhausted"
	ReasonDataCapReached  = "data cap reached"
	ReasonIdle            = "idle"
	ReasonLeaseNotFunded  = "lease not funded"
	ReasonNoEnforcer      = "no enforcer"
	ReasonOperator        = "closed by operator"
	ReasonPositionClosed  = "position closed on chain"
	ReasonPositionClosing = "position closing"
	ReasonReplaced        = "replaced"
	ReasonRetry           = "retrying close"
	ReasonTicketExpired   = "ticket expired"
)

//nolint:gochecknoglobals
var spec = sync.OnceValue(func() *openapi3.T {
	s, err := GetSpec()
	if err != nil {
		panic(fmt.Errorf("loading nodeapi spec: %w", err))
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
		panic(fmt.Errorf("loading nodeapi client spec: %w", err))
	}

	api.OpenErrorCodes(s)

	return s
})

type Unimplemented struct{}

func (Unimplemented) CloseSession(context.Context, CloseSessionRequestObject) (CloseSessionResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetChannel(context.Context, GetChannelRequestObject) (GetChannelResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetHealth(context.Context, GetHealthRequestObject) (GetHealthResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetLease(context.Context, GetLeaseRequestObject) (GetLeaseResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetLeaseEvents(
	context.Context,
	GetLeaseEventsRequestObject,
) (GetLeaseEventsResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetMetadata(context.Context, GetMetadataRequestObject) (GetMetadataResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetSession(context.Context, GetSessionRequestObject) (GetSessionResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) GetSessionEvents(
	context.Context,
	GetSessionEventsRequestObject,
) (GetSessionEventsResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) OpenSession(context.Context, OpenSessionRequestObject) (OpenSessionResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) SendChannelVoucher(
	context.Context,
	SendChannelVoucherRequestObject,
) (SendChannelVoucherResponseObject, error) {
	return nil, api.ErrInternal
}

func (Unimplemented) SendLeaseVoucher(
	context.Context,
	SendLeaseVoucherRequestObject,
) (SendLeaseVoucherResponseObject, error) {
	return nil, api.ErrInternal
}

func Discover(
	ctx context.Context,
	programClient *program.Client,
	nodeAddress solana.PublicKey,
	node dvpn.Node,
	timeout time.Duration,
	opts ...api.TransportOption,
) (Metadata, api.PeerURL, error) {
	uri, err := api.ParsePeerURL(program.NodeURI(node))
	if err != nil {
		return Metadata{}, api.PeerURL{}, fmt.Errorf("parsing node uri: %w", err)
	}

	client, err := NewInsecureClient(uri.String(), timeout, opts...)
	if err != nil {
		return Metadata{}, api.PeerURL{}, fmt.Errorf("building insecure client: %w", err)
	}

	resp, err := client.GetMetadataWithResponse(ctx)
	if err != nil {
		return Metadata{}, api.PeerURL{}, fmt.Errorf("fetching node metadata from %s: %w", uri, err)
	}

	if resp.JSON200 == nil {
		err := api.ParseError(resp.StatusCode(), resp.Body)

		return Metadata{}, api.PeerURL{}, fmt.Errorf("fetching node metadata from %s: %w", uri, err)
	}

	metadata, err := VerifyMetadata(
		ctx,
		programClient,
		*resp.JSON200,
		nodeAddress,
		node.Owner,
		programClient.ProgramID,
		time.Now(),
	)
	if err != nil {
		return Metadata{}, api.PeerURL{}, fmt.Errorf("verifying node metadata: %w", err)
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
	key solana.PrivateKey,
	timeout time.Duration,
	opts ...api.TransportOption,
) (*ClientWithResponses, error) {
	pinned := api.NewPinnedHTTPClient(fingerprint, timeout, opts...)
	validated := api.NewValidatedTransport(pinned.Transport, clientSpec(), api.MaxResponseBodyBytes)
	signed := &api.SignedTransport{
		Base:   validated,
		Signer: key,
	}
	pinned.Transport = &api.RetryTransport{Base: signed}

	return NewClientWithResponses(baseURL, WithHTTPClient(pinned))
}

func RegisterRoutes(
	engine *gin.Engine,
	svc StrictServerInterface,
	seen *ttlcache.Cache[auth.Nonce, struct{}],
	byIP,
	bySigner *api.Limiter,
) {
	requireSignature := api.RequireSignature(seen)
	limitSigner := api.LimitBySigner(bySigner)
	unsignedPaths := map[string]bool{
		"/v1/health":   true,
		"/v1/metadata": true,
	}

	signedOnly := func(c *gin.Context) {
		if unsignedPaths[c.FullPath()] {
			c.Next()

			return
		}

		requireSignature(c)

		if !c.IsAborted() {
			limitSigner(c)
		}
	}

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
	ginMiddlewares := []MiddlewareFunc{limitIP, signedOnly, validate}
	ginOptions := GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, _ int) {
			api.AbortRequest(c, err)
		},
		Middlewares: ginMiddlewares,
	}

	RegisterHandlersWithOptions(engine, handler, ginOptions)
}

func SignMetadata(key solana.PrivateKey, metadata Metadata) (auth.SignedMessage, error) {
	signed, err := auth.SignMessage(key, auth.DomainNodeMetadata, metadata)
	if err != nil {
		return auth.SignedMessage{}, fmt.Errorf("signing metadata: %w", err)
	}

	return signed, nil
}

func StreamingPaths() []string {
	return []string{"/v1/leases/:lease/events", "/v1/sessions/:id/events"}
}

func VerifyMetadata(
	ctx context.Context,
	src auth.GrantSource,
	signed auth.SignedMessage,
	node,
	owner,
	programID solana.PublicKey,
	now time.Time,
) (Metadata, error) {
	expected := auth.MetadataClaims{
		Operator:  signed.Signer,
		Owner:     owner,
		ProgramID: programID,
		Subject:   node,
		Version:   auth.MetadataVersion,
	}
	check := auth.MetadataCheck{
		Domain:     auth.DomainNodeMetadata,
		Expected:   expected,
		Now:        now,
		Permission: program.PermSignNodeMetadata,
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
		Subject:     metadata.Node,
		Version:     metadata.Version,
	}
}

func metadataSchema() *openapi3.Schema {
	return spec().Components.Schemas["Metadata"].Value
}
