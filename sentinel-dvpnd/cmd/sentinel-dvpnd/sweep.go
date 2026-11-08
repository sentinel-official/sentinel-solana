package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/programs/system"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/daemon"
)

// ─── key sweep ───

const (
	sweepConfirmTimeout = 90 * time.Second
	sweepPollInterval   = time.Second
)

var (
	errSweepEmpty    = errors.New("nothing to sweep")
	errSweepFailed   = errors.New("sweep transaction failed")
	errSweepNoFee    = errors.New("the rpc returned no fee for the sweep")
	errSweepTimedOut = errors.New("sweep transaction not confirmed in time")
	errSweepToSelf   = errors.New("--to is the instance's own key")
)

// SweepResult is what `key sweep` prints.
type SweepResult struct {
	DryRun    bool   `json:"dry_run,omitempty"`
	Fee       uint64 `json:"fee_lamports"`
	From      string `json:"from"`
	Lamports  uint64 `json:"lamports"`
	Signature string `json:"signature,omitempty"`
	To        string `json:"to"`
}

func newKeySweepCmd(opts *instanceOptions) *cobra.Command {
	var (
		dryRun bool
		to     string
	)

	cmd := &cobra.Command{
		Args: cobra.NoArgs,
		Long: "Send all of the instance key's SOL, less the network fee, to another address, such as the owner " +
			"wallet. Run it with the daemon stopped (it takes the instance lock), for example before retiring a device.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			result, err := sweepKey(cmd.Context(), opts, cmd.Flags(), to, dryRun)
			if err != nil {
				return err
			}

			return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
		},
		Short: "send the instance key's SOL to another address",
		Use:   "sweep",
	}
	cmd.Flags().StringVar(&to, "to", "", "destination `address`, such as the owner wallet")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show the amount and fee without sending")
	config.AddFlags(cmd.Flags())

	if err := cmd.MarkFlagRequired("to"); err != nil {
		panic(fmt.Sprintf("marking --to required: %v", err))
	}

	return cmd
}

func sweepKey(ctx context.Context, opts *instanceOptions, flags *pflag.FlagSet, to string, dryRun bool) (SweepResult, error) {
	dest, err := solana.PublicKeyFromBase58(to)
	if err != nil {
		return SweepResult{}, fmt.Errorf("parsing --to: %w", err)
	}

	loaded, err := opts.load(flags)
	if err != nil {
		return SweepResult{}, err
	}

	// The lock keeps a running daemon from losing its fee SOL mid-session.
	release, err := daemon.Lock(loaded.Dir)
	if err != nil {
		return SweepResult{}, fmt.Errorf("locking instance folder (stop the daemon first): %w", err)
	}
	defer func() { _ = release() }()

	privateKey, err := loadPrivateKey(loaded)
	if err != nil {
		return SweepResult{}, err
	}

	if privateKey.PublicKey().Equals(dest) {
		return SweepResult{}, errSweepToSelf
	}

	return sweep(ctx, loaded.File.RPC.NewClient(), privateKey, dest, loaded.File.RPC.Commitment, dryRun)
}

// sweep sends balance minus fee in a transaction holding one system transfer and no compute-budget
// instructions, so the fee the rpc quotes for the message is the whole cost and the key ends at zero.
func sweep(
	ctx context.Context,
	rpcClient *rpc.Client,
	signer solana.PrivateKey,
	dest solana.PublicKey,
	commitment rpc.CommitmentType,
	dryRun bool,
) (SweepResult, error) {
	from := signer.PublicKey()
	result := SweepResult{DryRun: dryRun, From: from.String(), To: dest.String()}

	balance, err := lamportsOf(ctx, rpcClient, from, commitment)
	if err != nil {
		return result, err
	}

	hash, err := rpcClient.GetLatestBlockhash(ctx, commitment)
	if err != nil {
		return result, fmt.Errorf("fetching latest blockhash: %w", err)
	}

	blockhash := hash.Value.Blockhash

	fee, err := sweepFee(ctx, rpcClient, from, dest, blockhash, commitment)
	if err != nil {
		return result, err
	}

	result.Fee = fee

	if balance <= fee {
		return result, fmt.Errorf("%w: %s holds %d lamports and the fee is %d", errSweepEmpty, from, balance, fee)
	}

	result.Lamports = balance - fee
	if dryRun {
		return result, nil
	}

	t, err := sweepTx(from, dest, result.Lamports, blockhash)
	if err != nil {
		return result, err
	}

	if _, err := t.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(from) {
			return &signer
		}

		return nil
	}); err != nil {
		return result, fmt.Errorf("signing sweep: %w", err)
	}

	sig, err := rpcClient.SendTransactionWithOpts(ctx, t, rpc.TransactionOpts{PreflightCommitment: commitment})
	if err != nil {
		return result, fmt.Errorf("sending sweep: %w", err)
	}

	result.Signature = sig.String()

	return result, awaitSweep(ctx, rpcClient, sig, commitment)
}

func lamportsOf(ctx context.Context, rpcClient *rpc.Client, address solana.PublicKey, commitment rpc.CommitmentType) (uint64, error) {
	account, err := rpcClient.GetAccountInfoWithOpts(ctx, address, &rpc.GetAccountInfoOpts{Commitment: commitment})
	if errors.Is(err, rpc.ErrNotFound) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", address, err)
	}

	return account.Value.Lamports, nil
}

func sweepFee(
	ctx context.Context,
	rpcClient *rpc.Client,
	from, dest solana.PublicKey,
	blockhash solana.Hash,
	commitment rpc.CommitmentType,
) (uint64, error) {
	// The amount does not change the fee; quote the message with a placeholder of 1 lamport.
	probe, err := sweepTx(from, dest, 1, blockhash)
	if err != nil {
		return 0, err
	}

	raw, err := probe.Message.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("serializing sweep message: %w", err)
	}

	quote, err := rpcClient.GetFeeForMessage(ctx, base64.StdEncoding.EncodeToString(raw), commitment)
	if err != nil {
		return 0, fmt.Errorf("quoting the sweep fee: %w", err)
	}

	if quote == nil || quote.Value == nil {
		return 0, errSweepNoFee
	}

	return *quote.Value, nil
}

func sweepTx(from, dest solana.PublicKey, lamports uint64, blockhash solana.Hash) (*solana.Transaction, error) {
	ix := system.NewTransferInstruction(lamports, from, dest).Build()

	t, err := solana.NewTransaction([]solana.Instruction{ix}, blockhash, solana.TransactionPayer(from))
	if err != nil {
		return nil, fmt.Errorf("building sweep: %w", err)
	}

	return t, nil
}

func awaitSweep(ctx context.Context, rpcClient *rpc.Client, sig solana.Signature, commitment rpc.CommitmentType) error {
	ctx, cancel := context.WithTimeout(ctx, sweepConfirmTimeout)
	defer cancel()

	// A failed status read is retried until the deadline; the last one is reported with the timeout.
	var lastErr error

	for {
		resp, err := rpcClient.GetSignatureStatuses(ctx, true, sig)
		if err != nil {
			lastErr = err
		} else if resp != nil && len(resp.Value) == 1 && resp.Value[0] != nil {
			status := resp.Value[0]
			if status.Err != nil {
				return fmt.Errorf("%w: %s: %v", errSweepFailed, sig, status.Err)
			}

			if reachedCommitment(status.ConfirmationStatus, commitment) {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s (check it before sweeping again): %w", errSweepTimedOut, sig, errors.Join(ctx.Err(), lastErr))
		case <-time.After(sweepPollInterval):
		}
	}
}

func reachedCommitment(got rpc.ConfirmationStatusType, want rpc.CommitmentType) bool {
	switch want {
	case rpc.CommitmentFinalized:
		return got == rpc.ConfirmationStatusFinalized
	case rpc.CommitmentConfirmed:
		return got == rpc.ConfirmationStatusConfirmed || got == rpc.ConfirmationStatusFinalized
	default:
		return got != ""
	}
}
