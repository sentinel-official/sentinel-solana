package api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
)

const MaxResponseBodyBytes = 1 << 20

const eventStreamMediaType = "text/event-stream"

var (
	ErrInvalidResponse  = errors.New("invalid response")
	ErrResponseTooLarge = errors.New("response too large")
)

type ValidatedTransport struct {
	Base http.RoundTripper

	maxBytes int64
	router   routers.Router
}

func NewValidatedTransport(base http.RoundTripper, spec *openapi3.T, maxBytes int64) *ValidatedTransport {
	return &ValidatedTransport{
		Base:     base,
		maxBytes: maxBytes,
		router:   newSchemaRouter(spec),
	}
}

func (t *ValidatedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	route, params, err := t.router.FindRoute(req)
	if err != nil {
		return nil, fmt.Errorf("finding route %s %s: %w", req.Method, req.URL.Path, err)
	}

	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}

	resp, err := base.RoundTrip(req)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	if resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest {
		_, _ = io.CopyN(io.Discard, resp.Body, retryDrainLimitBytes)
		_ = resp.Body.Close()

		return nil, fmt.Errorf("%w: %s %s: status %d", ErrUnexpectedRedirect, req.Method, req.URL.Path, resp.StatusCode)
	}

	if resp.StatusCode == http.StatusNoContent || isEventStream(route, resp) {
		return resp, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, t.maxBytes+1))
	_ = resp.Body.Close()

	if err != nil {
		return nil, fmt.Errorf("reading response body %s %s: %w", req.Method, req.URL.Path, err)
	}

	if int64(len(body)) > t.maxBytes {
		return nil, fmt.Errorf("%w: %s %s: over %d bytes", ErrResponseTooLarge, req.Method, req.URL.Path, t.maxBytes)
	}

	checked, err := forwardCompatibleBody(route, resp, body)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s: %s", ErrInvalidResponse, req.Method, req.URL.Path, boundErrorText(err.Error()))
	}

	options := &openapi3filter.Options{IncludeResponseStatus: true}
	options.WithCustomSchemaErrorFunc(formatSchemaError)

	input := &openapi3filter.ResponseValidationInput{
		Body:    io.NopCloser(bytes.NewReader(checked)),
		Header:  resp.Header,
		Options: options,
		RequestValidationInput: &openapi3filter.RequestValidationInput{
			Options:    &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
			PathParams: params,
			Request:    req,
			Route:      route,
		},
		Status: resp.StatusCode,
	}
	if err := openapi3filter.ValidateResponse(req.Context(), input); err != nil {
		text := boundErrorText(err.Error())

		return nil, fmt.Errorf("%w: %s %s: %s", ErrInvalidResponse, req.Method, req.URL.Path, text)
	}

	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))

	return resp, nil
}

func isEventStream(route *routers.Route, resp *http.Response) bool {
	if resp.StatusCode != http.StatusOK {
		return false
	}

	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != eventStreamMediaType {
		return false
	}

	ok := route.Operation.Responses.Status(http.StatusOK)
	if ok == nil || ok.Value == nil {
		return false
	}

	_, found := ok.Value.Content[eventStreamMediaType]

	return found
}
