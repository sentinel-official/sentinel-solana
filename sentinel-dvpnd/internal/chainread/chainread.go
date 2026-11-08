// Package chainread reads many program accounts in as few RPC calls as possible.
//
// core.FetchBatch fails the whole batch when one account is missing, and a closed channel or
// lease is exactly that, so callers read one account per call instead. A node did that for every
// open position every minute (sync_positions), so its RPC load grew with every deposit it holds.
// Here a missing or foreign account is reported per item and the batch still succeeds.
package chainread

import (
	"context"
	"fmt"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"

	"github.com/sentinel-official/sentinel-programs/clients/go/core"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// MaxBatch is the most addresses one getMultipleAccounts call may carry.
const MaxBatch = 100

// Result for one address: Account when Err is nil; Err wraps core.ErrAccountNotFound or
// core.ErrOwnerMismatch for a missing or foreign account, or the decode error.
type Result[T any] struct {
	Account core.Keyed[T]
	Err     error
}

// Channels reads channel accounts, MaxBatch per call. The error is for the call itself (an RPC
// failure such as a 429); per-account problems are in each Result.
func Channels(ctx context.Context, c *dvpn.Client, addresses []solana.PublicKey) ([]Result[dvpn.Channel], error) {
	return fetch(ctx, c, addresses, dvpn.ParseChannelAccount)
}

// Leases reads lease accounts, MaxBatch per call. See Channels.
func Leases(ctx context.Context, c *dvpn.Client, addresses []solana.PublicKey) ([]Result[dvpn.Lease], error) {
	return fetch(ctx, c, addresses, dvpn.ParseLeaseAccount)
}

func fetch[T any](
	ctx context.Context,
	c *dvpn.Client,
	addresses []solana.PublicKey,
	parse func([]byte) (*T, error),
) ([]Result[T], error) {
	out := make([]Result[T], len(addresses))
	opts := c.Options()

	for start := 0; start < len(addresses); start += MaxBatch {
		end := min(start+MaxBatch, len(addresses))

		result, err := c.RPC.GetMultipleAccountsWithOpts(ctx, addresses[start:end], &rpc.GetMultipleAccountsOpts{
			Commitment: opts.Commitment,
			Encoding:   solana.EncodingBase64,
		})
		if err != nil {
			return nil, err
		}

		if len(result.Value) != end-start {
			return nil, fmt.Errorf("expected %d accounts, got %d", end-start, len(result.Value))
		}

		for i, account := range result.Value {
			idx := start + i
			address := addresses[idx]
			out[idx].Account.Address = address
			out[idx].Account.Slot = result.Context.Slot

			switch {
			case account == nil:
				out[idx].Err = fmt.Errorf("%w: %s", core.ErrAccountNotFound, address)
			case !opts.Owner.IsZero() && !account.Owner.Equals(opts.Owner):
				out[idx].Err = fmt.Errorf("%w: %s", core.ErrOwnerMismatch, address)
			default:
				value, err := parse(account.Data.GetBinary())
				if err != nil {
					out[idx].Err = fmt.Errorf("%s: %w", address, err)

					continue
				}

				out[idx].Account.Value = *value
			}
		}
	}

	return out, nil
}
