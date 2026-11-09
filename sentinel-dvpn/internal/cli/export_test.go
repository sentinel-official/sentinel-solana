package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/spf13/cobra"

	"github.com/sentinel-official/sentinel-dvpn/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-programs/clients/go/core"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

var ErrTargetAmbiguous = errTargetAmbiguous

type NodeInspectResult = nodeInspectResult

type Runner = runner

func NewRunner(programClient *program.Client, sender *tx.Sender, signer *SignerKey, printer *Printer) *Runner {
	return newRunner(programClient, sender, signer, printer)
}

type SignerKey = signerKey

func BuildCredential(
	channel string,
	newChannel bool,
	mode *uint32,
	escrowBaseUnits uint64,
	requestTicket bool,
	provider string,
	subscription string,
	ticketHash string,
) (*daemonv1.Credential, error) {
	return buildCredential(startFlags{
		channel:         channel,
		escrowBaseUnits: escrowBaseUnits,
		mode:            mode,
		newChannel:      newChannel,
		provider:        provider,
		requestTicket:   requestTicket,
		subscription:    subscription,
		ticketHash:      ticketHash,
	})
}

func DialerDial(endpoint, stateRoot string, ctx context.Context, tag instance.Tag) error {
	daemon := &dialer{
		endpoint:  endpoint,
		stateRoot: stateRoot,
	}

	conn, err := daemon.Dial(ctx, tag)
	if err != nil {
		return err
	}

	if err := conn.Close(); err != nil {
		return fmt.Errorf("closing connection: %w", err)
	}

	return nil
}

func DialerEndpointFor(endpoint string, tag instance.Tag) string {
	daemon := &dialer{endpoint: endpoint}

	return daemon.endpointFor(tag)
}

func InspectNode(
	ctx context.Context,
	programClient *program.Client,
	signer *SignerKey,
	nodeAddress solana.PublicKey,
	peer cli.PeerConfig,
) (NodeInspectResult, error) {
	return inspectNode(ctx, programClient, signer, nodeAddress, peer)
}

func NewChainGroups(runner *Runner) []*cobra.Command {
	return newChainGroups(runner)
}

func NewNodeSearchCmd(programClient *dvpn.Client, printer *Printer) *cobra.Command {
	return newNodeSearchCmd(programClient, printer)
}

func NewProgramStatusCmd(programClient *dvpn.Client, printer *Printer) *cobra.Command {
	return newProgramStatusCmd(programClient, printer)
}

func NewProviderSearchCmdForTest(programClient *dvpn.Client, printer *Printer) *cobra.Command {
	return newSearchCmd(programClient, printer, dvpn.FindProviderAccounts, providerFilters, providerListing)
}

func NewShowCmd[T any](
	programClient *dvpn.Client,
	printer *Printer,
	name string,
	fetch func(context.Context, *rpc.Client, solana.PublicKey, core.Options) (core.Keyed[T], error),
) *cobra.Command {
	return newShowCmd(programClient, printer, name, fetch)
}

func NewSignerKeyForTest(privateKey solana.PrivateKey) *SignerKey {
	return &signerKey{privateKey: privateKey}
}

func NewTicketRequestCmd(
	programClient *program.Client,
	signer *SignerKey,
	peer cli.PeerConfig,
	printer *Printer,
) *cobra.Command {
	return newTicketRequestCmd(programClient, signer, &config.File{Peer: peer}, printer)
}

func NewTicketVerifyCmd(programClient *program.Client, printer *Printer) *cobra.Command {
	return newTicketVerifyCmd(programClient, printer)
}

func NewWalletApproveDelegateCmd(
	programClient *program.Client,
	sender *tx.Sender,
	signer *SignerKey,
	printer *Printer,
) *cobra.Command {
	return newWalletApproveDelegateCmd(programClient, sender, signer, printer)
}

func NewWalletCreateTokenAccountCmd(
	programClient *program.Client,
	sender *tx.Sender,
	signer *SignerKey,
	printer *Printer,
) *cobra.Command {
	return newWalletCreateTokenAccountCmd(programClient, sender, signer, printer)
}

func ParseApproveAmount(amount string) (uint64, error) {
	return parseApproveAmount(amount)
}

func Prepare(runner *Runner, cmd *cobra.Command) error {
	return runner.prepare(cmd, nil)
}

func ToSendResult(receipt *tx.Receipt) any {
	return toSendResult(receipt)
}

func WrapCallError(role string, tag instance.Tag, endpoint string, err error, ifaces func() (map[string]bool, error), liveIface func() string) error {
	return wrapCallError(role, tag, endpoint, err, ifaces, liveIface)
}

func DialerTunnelIface(stateRoot string, tag instance.Tag) string {
	daemon := &dialer{stateRoot: stateRoot}

	return daemon.tunnelIface(tag)
}

// CallError runs a dialer's callError with a debug logger writing to w.
func CallError(w io.Writer, endpoint, role string, tag instance.Tag, err error) error {
	d := &dialer{endpoint: endpoint, log: zerolog.New(w).Level(zerolog.DebugLevel)}

	return d.callError(role, tag, err)
}
