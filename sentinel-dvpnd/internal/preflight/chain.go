package preflight

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-go-sdk/v3/key"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

var errMissingBlockTime = errors.New("rpc returned no block time")

func checkChain(ctx context.Context, loaded *config.Loaded, programClient *program.Client) []service.PreflightCheck {
	checks := []service.PreflightCheck{
		newCheck("mint", programClient.CheckMint(ctx)),
		newCheck("clock_skew", checkClockSkew(ctx, programClient.RPC)),
	}
	signer, err := loadSigner(loaded)

	checks = append(checks, newCheck("key", err))
	if err == nil {
		owner, permissions, subjects, grantErr := grantFor(loaded, programClient.ProgramID)
		if grantErr == nil {
			grantErr = checkGrant(ctx, programClient, owner, signer, permissions, subjects...)
		}

		checks = append(checks, newCheck("grant", grantErr))
	}

	return checks
}

func checkClockSkew(ctx context.Context, rpcClient *rpc.Client) error {
	slot, err := rpcClient.GetSlot(ctx, rpc.CommitmentConfirmed)
	if err != nil {
		return fmt.Errorf("fetching slot: %w", err)
	}

	blockTime, err := rpcClient.GetBlockTime(ctx, slot)
	if err != nil {
		return fmt.Errorf("fetching block time: %w", err)
	}

	if blockTime == nil {
		return errMissingBlockTime
	}

	skew := time.Since(blockTime.Time())
	if skew > auth.MaxRequestSkew || skew < -auth.MaxRequestSkew {
		return fmt.Errorf("local clock is %s away from chain time", skew)
	}

	return nil
}

func checkGrant(
	ctx context.Context,
	programClient *program.Client,
	owner string,
	signer solana.PublicKey,
	permissions uint64,
	subjects ...solana.PublicKey,
) error {
	ownerKey, err := solana.PublicKeyFromBase58(owner)
	if err != nil {
		return fmt.Errorf("parsing owner: %w", err)
	}

	if err := auth.Authorize(ctx, programClient, ownerKey, signer, permissions, subjects...); err != nil {
		if errors.Is(err, auth.ErrUnauthorized) {
			allowance := dvpn.AllowanceUnlimited
			if permissions&^program.PermsNode == 0 {
				allowance = 0
			}

			return fmt.Errorf("%s holds no grant from %s covering permissions %#x; run %s: %w",
				signer, ownerKey, permissions, grantHint(signer, permissions, allowance), err)
		}

		return err
	}

	return nil
}

func entityAddress(
	programID solana.PublicKey,
	owner, id string,
	find func(solana.PublicKey, solana.PublicKey, [32]uint8) (solana.PublicKey, uint8, error),
) ([]solana.PublicKey, error) {
	ownerKey, err := solana.PublicKeyFromBase58(owner)
	if err != nil {
		return nil, fmt.Errorf("parsing owner: %w", err)
	}

	raw, err := program.ParseID(id)
	if err != nil {
		return nil, err
	}

	address, _, err := find(programID, ownerKey, raw)
	if err != nil {
		return nil, fmt.Errorf("deriving entity address: %w", err)
	}

	return []solana.PublicKey{address}, nil
}

func grantFor(loaded *config.Loaded, programID solana.PublicKey) (string, uint64, []solana.PublicKey, error) {
	switch loaded.Role {
	case config.RoleClient:
		return loaded.Client.Owner, program.PermsClient, nil, nil
	case config.RoleNode:
		subject, err := entityAddress(programID, loaded.Node.Owner, loaded.Node.ID, dvpn.FindNodeAddress)

		return loaded.Node.Owner, program.PermsNode, subject, err
	case config.RoleProvider:
		subject, err := entityAddress(programID, loaded.Provider.Owner, loaded.Provider.ID, dvpn.FindProviderAddress)

		return loaded.Provider.Owner, program.PermsProvider, subject, err
	default:
		return "", 0, nil, nil
	}
}

func loadSigner(loaded *config.Loaded) (solana.PublicKey, error) {
	path, err := loaded.KeyPath()
	if err != nil {
		return solana.PublicKey{}, err
	}

	passphrase, err := loaded.LoadPassphrase()
	if err != nil {
		return solana.PublicKey{}, err
	}

	privateKey, err := key.Load(path, passphrase)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("loading key: %w", err)
	}

	return privateKey.PublicKey(), nil
}

// grantHint is the command that gives signer the grant it lacks. The owner signs it, so the key is
// selected with sentinel-dvpn's --key.name flag (the CLI has no --key).
func grantHint(signer solana.PublicKey, permissions, allowance uint64) string {
	return fmt.Sprintf("sentinel-dvpn grant open-grant --key.name <owner> --grantee %s --target %s "+
		"--permissions %d --expires-at 0 --allowance %d", signer, solana.PublicKey{}, permissions, allowance)
}
