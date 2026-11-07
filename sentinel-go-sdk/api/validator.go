package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/gin-gonic/gin"
)

const maxSchemaErrorBytes = 256

// ValidateJSON checks a message read from a peer against schema. Keys the schema does not declare are ignored
// (stripUnknown), so a peer on a newer version can add a field.
func ValidateJSON(schema *openapi3.Schema, raw []byte) error {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("decoding json: %w", err)
	}

	if _, err := stripUnknown(schema, value, nil); err != nil {
		return errors.New(boundErrorText(err.Error()))
	}

	if err := schema.VisitJSON(value, openapi3.SetSchemaErrorMessageCustomizer(formatSchemaError)); err != nil {
		return errors.New(boundErrorText(err.Error()))
	}

	return nil
}

func ValidateRequests(spec *openapi3.T) gin.HandlerFunc {
	router := newSchemaRouter(spec)
	options := &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc}

	return func(c *gin.Context) {
		route, params, err := router.FindRoute(c.Request)
		if err != nil {
			AbortRequest(c, err)

			return
		}

		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			AbortRequest(c, err)

			return
		}

		c.Request.Body = io.NopCloser(bytes.NewReader(body))

		input := &openapi3filter.RequestValidationInput{
			Options:    options,
			PathParams: params,
			Request:    c.Request,
			Route:      route,
		}
		if err := openapi3filter.ValidateRequest(c.Request.Context(), input); err != nil {
			AbortRequest(c, err)

			return
		}

		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		c.Next()
	}
}

func boundErrorText(text string) string {
	printable := strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return '?'
		}

		return r
	}, text)

	if len(printable) > maxSchemaErrorBytes {
		return printable[:maxSchemaErrorBytes] + "..."
	}

	return printable
}

func formatSchemaError(err *openapi3.SchemaError) string {
	reason := err.Reason
	if reason == "" {
		reason = "does not match " + err.SchemaField
	}

	return boundErrorText("at /" + strings.Join(err.JSONPointer(), "/") + ": " + reason)
}

func newSchemaRouter(spec *openapi3.T) routers.Router {
	router, err := gorillamux.NewRouter(spec)
	if err != nil {
		panic(err)
	}

	return router
}
