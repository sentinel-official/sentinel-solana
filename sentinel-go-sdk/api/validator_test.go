package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
)

func TestValidateJSON(t *testing.T) {
	t.Parallel()

	name := openapi3.NewStringSchema().WithMaxLength(8)
	schema := openapi3.NewObjectSchema().WithProperty("name", name).WithoutAdditionalProperties()

	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{
			name: "valid",
			raw:  `{"name":"abc"}`,
		},
		{
			name:    "malformed",
			raw:     `{`,
			wantErr: true,
		},
		{
			name:    "huge value",
			raw:     `{"name":"` + strings.Repeat(`\u001b`, 100000) + `"}`,
			wantErr: true,
		},
		{
			name: "huge unknown key is ignored",
			raw:  `{"name":"abc","` + strings.Repeat(`\u001b`, 100000) + `":1}`,
		},
		{
			name:    "case variant key",
			raw:     `{"name":"abc","NAME":"` + strings.Repeat(`\u001b`, 100000) + `"}`,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := api.ValidateJSON(schema, []byte(tc.raw))
			if !tc.wantErr {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.LessOrEqual(t, len(err.Error()), 300)
			require.NotContains(t, err.Error(), "\x1b")
		})
	}
}

//nolint:paralleltest
func TestValidateRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)

	spec, err := openapi3.NewLoader().LoadFromFile("testdata/validator.yaml")
	require.NoError(t, err)

	cases := []struct {
		body        string
		contentType string
		method      string
		name        string
		path        string
		status      int
	}{
		{
			body:        `{"name":"abc"}`,
			contentType: "application/json",
			method:      http.MethodPost,
			name:        "valid body",
			path:        "/v1/things",
			status:      http.StatusOK,
		},
		{
			body:        `{"name":"abcd"}`,
			contentType: "application/json",
			method:      http.MethodPost,
			name:        "body over max length",
			path:        "/v1/things",
			status:      http.StatusBadRequest,
		},
		{
			body:        `{"name":"abc"}`,
			contentType: "text/plain",
			method:      http.MethodPost,
			name:        "valid json with text content type",
			path:        "/v1/things",
			status:      http.StatusBadRequest,
		},
		{
			method: http.MethodGet,
			name:   "path param off pattern",
			path:   "/v1/things/ABC",
			status: http.StatusBadRequest,
		},
		{
			method: http.MethodGet,
			name:   "valid path param",
			path:   "/v1/things/abc",
			status: http.StatusOK,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ran bool

			engine := api.NewEngine(api.EngineOptions{Logger: zerolog.Nop()})
			handler := func(c *gin.Context) {
				ran = true

				c.Status(http.StatusOK)
			}
			engine.POST("/v1/things", api.ValidateRequests(spec), handler)
			engine.GET("/v1/things/:id", api.ValidateRequests(spec), handler)

			req := httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, strings.NewReader(tc.body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}

			recorder := httptest.NewRecorder()
			engine.ServeHTTP(recorder, req)

			require.Equal(t, tc.status, recorder.Code)
			require.Equal(t, tc.status == http.StatusOK, ran)

			if tc.status != http.StatusOK {
				var response api.ErrorResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
				require.Equal(t, api.CodeBadRequest, response.Error.Code)
			}
		})
	}
}
