package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/routers"
)

// CodePattern bounds an error code a client reads but does not know: a newer peer may add codes.
const CodePattern = `^[a-z][a-z0-9_]{0,63}$`

//nolint:gochecknoglobals
var codePattern = regexp.MustCompile(CodePattern)

// OpenErrorCodes lets a client spec accept an error code outside its list, as long as it matches CodePattern.
// Call it on a spec loaded for the client only: the list stays the contract for what a node sends.
func OpenErrorCodes(spec *openapi3.T) {
	code := spec.Components.Schemas["ErrorCode"]
	if code == nil || code.Value == nil {
		return
	}

	code.Value.Enum = nil
	code.Value.Pattern = CodePattern
}

// stripUnknown removes the object keys a schema does not declare where it sets additionalProperties: false, so
// that a peer on a newer version can add a field and an older one ignores it instead of refusing the message.
// The required, type and range checks still apply to what is left. A key that matches a declared property only
// without regard to case is refused, because encoding/json would decode it into that field. It reports whether it
// removed anything.
func stripUnknown(schema *openapi3.Schema, value any, at []string) (bool, error) {
	if schema == nil {
		return false, nil
	}

	switch v := value.(type) {
	case map[string]any:
		return stripObject(schema, v, at)
	case []any:
		if schema.Items == nil {
			return false, nil
		}

		stripped := false

		for i, item := range v {
			s, err := stripUnknown(schema.Items.Value, item, append(slices.Clone(at), strconv.Itoa(i)))
			if err != nil {
				return false, err
			}

			stripped = stripped || s
		}

		return stripped, nil
	default:
		return false, nil
	}
}

func stripObject(schema *openapi3.Schema, v map[string]any, at []string) (bool, error) {
	extra := schema.AdditionalProperties
	closed := extra.Has != nil && !*extra.Has
	stripped := false

	for key, item := range v {
		sub := schema.Properties[key]
		if sub == nil && extra.Schema != nil {
			sub = extra.Schema
		}

		if sub != nil {
			s, err := stripUnknown(sub.Value, item, append(slices.Clone(at), key))
			if err != nil {
				return false, err
			}

			stripped = stripped || s

			continue
		}

		if !closed {
			continue
		}

		for name := range schema.Properties {
			if strings.EqualFold(key, name) {
				return false, fmt.Errorf("at /%s: property %q is unsupported", strings.Join(at, "/"), key)
			}
		}

		delete(v, key)

		stripped = true
	}

	return stripped, nil
}

// forwardCompatibleBody returns the body the response is checked against: body itself, or, when it holds keys the
// response schema does not declare, a copy without them. The caller still gets the body as received.
func forwardCompatibleBody(route *routers.Route, resp *http.Response, body []byte) ([]byte, error) {
	ref := route.Operation.Responses.Status(resp.StatusCode)
	if ref == nil {
		ref = route.Operation.Responses.Default()
	}

	if ref == nil || ref.Value == nil {
		return body, nil
	}

	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return body, nil //nolint:nilerr // the schema check reports the content type
	}

	content := ref.Value.Content.Get(mediaType)
	if content == nil || content.Schema == nil {
		return body, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber() // numbers are written back as received

	var value any
	if err := decoder.Decode(&value); err != nil {
		return body, nil //nolint:nilerr // the schema check reports the malformed body
	}

	stripped, err := stripUnknown(content.Schema.Value, value, nil)
	if err != nil || !stripped {
		return body, err
	}

	out, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encoding the checked body: %w", err)
	}

	return out, nil
}
