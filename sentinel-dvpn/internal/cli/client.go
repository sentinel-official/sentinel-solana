package cli

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	progcli "github.com/sentinel-official/sentinel-programs/clients/go/cli"
)

var (
	ErrCredentialEmpty     = errors.New("credential value must not be empty")
	ErrMissingChannelMode  = errors.New("mode is required with --new-channel")
	ErrMissingTicketFields = errors.New("provider and subscription are required with --ticket")
)

type startFlags struct {
	channel         string
	escrowBaseUnits uint64
	mode            *uint32
	newChannel      bool
	node            string
	provider        string
	requestTicket   bool
	subscription    string
	ticketHash      string
}

func buildCredential(flags startFlags) (*daemonv1.Credential, error) {
	switch {
	case flags.channel != "":
		return &daemonv1.Credential{Kind: &daemonv1.Credential_Channel{Channel: flags.channel}}, nil
	case flags.newChannel:
		if flags.mode == nil {
			return nil, ErrMissingChannelMode
		}

		newChannelValue := &daemonv1.NewChannel{
			Escrow: flags.escrowBaseUnits,
			Mode:   *flags.mode,
		}

		return &daemonv1.Credential{Kind: &daemonv1.Credential_NewChannel{NewChannel: newChannelValue}}, nil
	case flags.requestTicket:
		if flags.provider == "" || flags.subscription == "" {
			return nil, ErrMissingTicketFields
		}

		ticketRequest := &daemonv1.TicketRequest{
			Provider:     flags.provider,
			Subscription: flags.subscription,
		}

		return &daemonv1.Credential{Kind: &daemonv1.Credential_TicketRequest{TicketRequest: ticketRequest}}, nil
	case flags.ticketHash != "":
		return &daemonv1.Credential{Kind: &daemonv1.Credential_TicketHash{TicketHash: flags.ticketHash}}, nil
	default:
		return nil, ErrCredentialEmpty
	}
}

func newClientChannelsCmd(daemon *dialer, printer *Printer) *cobra.Command {
	spec := callSpec[daemonv1.ListChannelsResponse]{
		Args:  cobra.NoArgs,
		Role:  roleClient,
		Short: "list channels the daemon has opened",
		Use:   "channels",
	}

	return newUnaryCallCmd(
		daemon,
		printer,
		spec,
		daemonv1.NewClientServiceClient,
		daemonv1.ClientServiceClient.ListChannels,
		nil,
	)
}

func newClientCmd(daemon *dialer, printer *Printer) *cobra.Command {
	cmd := &cobra.Command{
		Short: "manage sessions through the client daemon",
		Use:   "client",
	}
	cmd.AddCommand(
		newClientChannelsCmd(daemon, printer),
		newClientSessionsCmd(daemon, printer),
		newClientStartCmd(daemon, printer),
		newClientStatusCmd(daemon, printer),
		newClientStopCmd(daemon, printer),
		newClientTicketsCmd(daemon, printer),
		newClientWatchCmd(daemon, printer),
	)

	return cmd
}

func newClientSessionsCmd(daemon *dialer, printer *Printer) *cobra.Command {
	spec := callSpec[daemonv1.ListSessionsResponse]{
		Args:  cobra.NoArgs,
		Role:  roleClient,
		Short: "list sessions known to the daemon",
		Use:   "sessions",
	}

	return newUnaryCallCmd(
		daemon,
		printer,
		spec,
		daemonv1.NewClientServiceClient,
		daemonv1.ClientServiceClient.ListSessions,
		nil,
	)
}

func newClientStartCmd(daemon *dialer, printer *Printer) *cobra.Command {
	var flags startFlags

	spec := callSpec[daemonv1.StartSessionResponse]{
		AddFlags: func(fs *pflag.FlagSet) {
			fs.StringVar(&flags.node, "node", "", "node `address` to connect to")
			fs.StringVar(&flags.channel, "channel", "", "existing channel `address` to spend from")
			fs.BoolVar(&flags.newChannel, "new-channel", false, "open a new channel for this session")
			progcli.OptionalVar(fs, &flags.mode, "mode", "new channel mode (voucher or stream)", "mode", parseMode)
			fs.Uint64Var(&flags.escrowBaseUnits, "escrow-base-units", 0, "new channel escrow `amount` in base units")
			fs.BoolVar(&flags.requestTicket, "ticket", false, "request a ticket through the daemon")
			fs.StringVar(&flags.provider, "provider", "", "provider `address` to request a ticket from")
			fs.StringVar(&flags.subscription, "subscription", "", "subscription `address` to request a ticket against")
			fs.StringVar(&flags.ticketHash, "ticket-hash", "", "hash of an already-issued ticket")
		},
		Args:  cobra.NoArgs,
		Role:  roleClient,
		Short: "start a session",
		Use:   "start",
	}
	newRequest := func(_ *cobra.Command, _ []string) (*daemonv1.StartSessionRequest, error) {
		credential, err := buildCredential(flags)
		if err != nil {
			return nil, err
		}

		return &daemonv1.StartSessionRequest{
			Credential: credential,
			Node:       flags.node,
		}, nil
	}
	cmd := newUnaryCallCmd(
		daemon,
		printer,
		spec,
		daemonv1.NewClientServiceClient,
		daemonv1.ClientServiceClient.StartSession,
		newRequest,
	)
	cmd.MarkFlagsMutuallyExclusive("channel", "new-channel", "ticket", "ticket-hash")
	cmd.MarkFlagsOneRequired("channel", "new-channel", "ticket", "ticket-hash")
	_ = cmd.MarkFlagRequired("node")

	return cmd
}

func newClientStatusCmd(daemon *dialer, printer *Printer) *cobra.Command {
	spec := callSpec[daemonv1.GetSessionResponse]{
		Args:  cobra.ExactArgs(1),
		Role:  roleClient,
		Short: "print a session's status",
		Use:   "status <id>",
	}
	newRequest := func(_ *cobra.Command, args []string) (*daemonv1.GetSessionRequest, error) {
		return &daemonv1.GetSessionRequest{Id: args[0]}, nil
	}

	return newUnaryCallCmd(
		daemon,
		printer,
		spec,
		daemonv1.NewClientServiceClient,
		daemonv1.ClientServiceClient.GetSession,
		newRequest,
	)
}

func newClientStopCmd(daemon *dialer, printer *Printer) *cobra.Command {
	var (
		keepKillSwitch bool
		nextNode       string
	)

	spec := callSpec[daemonv1.StopSessionResponse]{
		AddFlags: func(fs *pflag.FlagSet) {
			fs.BoolVar(&keepKillSwitch, "keep-kill-switch", false,
				"keep the kill switch in place for a session started right after (a server switch)")
			fs.StringVar(&nextNode, "next-node", "",
				"with --keep-kill-switch: node `address` the next session connects to (admitted to the kill switch)")
		},
		Args:  cobra.ExactArgs(1),
		Role:  roleClient,
		Short: "stop a session",
		Use:   "stop <id>",
	}
	newRequest := func(_ *cobra.Command, args []string) (*daemonv1.StopSessionRequest, error) {
		if nextNode != "" && !keepKillSwitch {
			return nil, errors.New("--next-node needs --keep-kill-switch")
		}

		return &daemonv1.StopSessionRequest{Id: args[0], KeepKillSwitch: keepKillSwitch, NextNode: nextNode}, nil
	}

	return newUnaryCallCmd(
		daemon,
		printer,
		spec,
		daemonv1.NewClientServiceClient,
		daemonv1.ClientServiceClient.StopSession,
		newRequest,
	)
}

func newClientTicketsCmd(daemon *dialer, printer *Printer) *cobra.Command {
	spec := callSpec[daemonv1.ListTicketsResponse]{
		Args:  cobra.NoArgs,
		Role:  roleClient,
		Short: "list tickets the daemon holds",
		Use:   "tickets",
	}

	return newUnaryCallCmd(
		daemon,
		printer,
		spec,
		daemonv1.NewClientServiceClient,
		daemonv1.ClientServiceClient.ListTickets,
		nil,
	)
}

func newClientWatchCmd(daemon *dialer, printer *Printer) *cobra.Command {
	spec := callSpec[daemonv1.WatchSessionResponse]{
		Args:  cobra.MaximumNArgs(1),
		Role:  roleClient,
		Short: "stream session updates",
		Use:   "watch [id]",
	}
	newRequest := func(_ *cobra.Command, args []string) (*daemonv1.WatchSessionRequest, error) {
		req := &daemonv1.WatchSessionRequest{}
		if len(args) == 1 {
			req.Id = args[0]
		}

		return req, nil
	}

	return newStreamCallCmd(
		daemon,
		printer,
		spec,
		daemonv1.NewClientServiceClient,
		daemonv1.ClientServiceClient.WatchSession,
		newRequest,
	)
}
