package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
)

func closedObject(props openapi3.Schemas, required ...string) *openapi3.Schema {
	s := openapi3.NewObjectSchema()
	s.Properties = props
	s.Required = required
	s.AdditionalProperties = openapi3.AdditionalProperties{Has: openapi3.Ptr(false)}

	return s
}

func TestValidateJSON_ForwardCompatible(t *testing.T) {
	t.Parallel()

	item := closedObject(openapi3.Schemas{"id": openapi3.NewStringSchema().NewRef()}, "id")
	labels := openapi3.NewObjectSchema()
	labels.AdditionalProperties = openapi3.AdditionalProperties{Has: openapi3.Ptr(true)}
	schema := closedObject(openapi3.Schemas{
		"items":  openapi3.NewArraySchema().WithItems(item).NewRef(),
		"labels": labels.NewRef(),
	}, "items")

	cases := []struct {
		body string
		name string
		ok   bool
	}{
		{body: `{"items":[{"id":"a","added":1}],"added":true}`, name: "unknown keys at both levels", ok: true},
		{body: `{"items":[{"id":"a"}],"labels":{"ID":"kept"}}`, name: "open map keeps any key", ok: true},
		{body: `{"items":[{"id":"a","ID":"b"}]}`, name: "case variant inside an array item"},
		{body: `{"items":[{"added":1}]}`, name: "required still required"},
		{body: `{"items":[{"id":7}]}`, name: "types still checked"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateJSON(schema, []byte(tc.body))
			if tc.ok {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
		})
	}
}

func TestParseError_UnknownCode(t *testing.T) {
	t.Parallel()

	body := func(code string) []byte {
		raw, err := json.Marshal(ErrorResponse{Error: Error{Code: code, Message: "m"}})
		require.NoError(t, err)

		return raw
	}

	var apiErr *Error
	require.True(t, errors.As(ParseError(http.StatusConflict, body("ticket_request_stale")), &apiErr))
	require.Equal(t, &Error{Code: "ticket_request_stale", HTTPStatus: http.StatusConflict, Message: "m"}, apiErr)
	require.NotErrorIs(t, apiErr, ErrConflict)

	require.ErrorIs(t, ParseError(http.StatusConflict, body("Bad Code")), ErrUnexpectedStatus)
	require.ErrorIs(t, ParseError(http.StatusConflict, body("")), ErrUnexpectedStatus)
}
