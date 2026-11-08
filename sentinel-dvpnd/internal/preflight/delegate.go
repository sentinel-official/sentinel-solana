package preflight

import (
	"context"
	"fmt"

	bin "github.com/gagliardetto/binary"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/programs/token"
	"github.com/solana-foundation/solana-go/v2/rpc"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// ApproveDelegateRemedy is the command that approves the program delegate on an owner's token account.
const ApproveDelegateRemedy = "run sentinel-dvpn wallet approve-delegate --amount-base-units unlimited"

func checkDelegate(ctx context.Context, loaded *config.Loaded, programClient *program.Client) service.PreflightCheck {
	ownerText, who := "", ""

	switch loaded.Role {
	case config.RoleProvider:
		ownerText, who = loaded.Provider.Owner, "provider"
	case config.RoleClient:
		ownerText, who = loaded.Client.Owner, "client"
	default:
		return newCheck("delegate", nil)
	}

	remedy := ApproveDelegateRemedy + " with the " + who + " owner key"

	owner, err := solana.PublicKeyFromBase58(ownerText)
	if err != nil {
		return newWarning("delegate", fmt.Sprintf("invalid %s owner: %v; %s", who, err, remedy))
	}

	if problem := DelegateProblem(ctx, programClient, owner); problem != "" {
		return newWarning("delegate", who+" "+problem+"; "+remedy)
	}

	return newCheck("delegate", nil)
}

// DelegateProblem says in plain words why the program cannot spend from owner's token account, or ""
// when the program delegate is approved with an allowance left. A failed read is a problem too: it
// is named, not hidden.
func DelegateProblem(ctx context.Context, programClient *program.Client, owner solana.PublicKey) string {
	address, err := programClient.AssociatedTokenAddress(owner)
	if err != nil {
		return fmt.Sprintf("owner token account could not be derived: %v", err)
	}

	resp, err := programClient.RPC.GetAccountInfoWithOpts(ctx, address, &rpc.GetAccountInfoOpts{
		Commitment: programClient.Commitment,
		Encoding:   solana.EncodingBase64,
	})
	if err != nil {
		return fmt.Sprintf("owner token account could not be read: %v", err)
	}

	if resp.Value.Owner != solana.Token2022ProgramID {
		return "owner token account is not owned by Token-2022"
	}

	var account token.Account
	if err := bin.NewBinDecoder(resp.GetBinary()).Decode(&account); err != nil {
		return fmt.Sprintf("owner token account could not be decoded: %v", err)
	}

	delegate, _, err := dvpn.FindConfigAddress(programClient.ProgramID)
	if err != nil {
		return fmt.Sprintf("delegate address could not be derived: %v", err)
	}

	if account.Delegate == nil || *account.Delegate != delegate || account.DelegatedAmount == 0 {
		return "delegate approval is missing or exhausted"
	}

	return ""
}
