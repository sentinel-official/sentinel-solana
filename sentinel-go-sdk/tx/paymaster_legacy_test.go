package tx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
)

func TestPaymasterConfig_ResolveLegacy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		in      tx.PaymasterConfig
		want    string
		wantErr error
	}{
		{name: "neither", in: tx.PaymasterConfig{}, want: ""},
		{name: "endpoint only", in: tx.PaymasterConfig{URL: "http://a"}, want: "http://a"},
		{name: "deprecated url only", in: tx.PaymasterConfig{LegacyURL: "http://a"}, want: "http://a"},
		{name: "both the same", in: tx.PaymasterConfig{URL: "http://a", LegacyURL: "http://a"}, want: "http://a"},
		{
			name:    "both different",
			in:      tx.PaymasterConfig{URL: "http://a", LegacyURL: "http://b"},
			wantErr: tx.ErrPaymasterURLConflict,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := tc.in
			err := cfg.ResolveLegacy()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.URL)
			require.Empty(t, cfg.LegacyURL)
		})
	}
}
