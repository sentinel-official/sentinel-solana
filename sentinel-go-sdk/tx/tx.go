package tx

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/solana-foundation/solana-go/v2/rpc/jsonrpc"
	"github.com/solana-foundation/solana-go/v2/rpc/ws"

	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
	"github.com/sentinel-official/sentinel-programs/clients/go/core"
)

const (
	PriorityFeeModeAuto  = "auto"
	PriorityFeeModeFixed = "fixed"
	PriorityFeeModeNever = "never"
)

const (
	defaultLoadedDataLimitBytes    = 8 << 20
	feeThresholdMicroLamports      = 1_000
	feeWindowSlots                 = 20
	maxComputeUnits                = 1_400_000
	microLamportsPerLamport        = 1_000_000
	minComputeUnits                = 5_000
	percent                        = 100
	rpcNodeUnhealthy               = -32005
	simulationComputeMarginPercent = 20
)

var (
	ErrBlockhashExpired     = errors.New("blockhash expired")
	ErrMissingInstructions  = errors.New("missing instructions")
	ErrOutcomeIndeterminate = errors.New("transaction outcome indeterminate")
	ErrRPCTransient         = errors.New("transient rpc error")
	ErrTransactionTooLarge  = errors.New("transaction too large")
)

type Config struct {
	Commitment               rpc.CommitmentType `json:"commitment"                  toml:"commitment"                  validate:"oneof=processed confirmed finalized"` //nolint:lll
	ComputeUnitMarginPercent uint8              `json:"compute_unit_margin_percent" toml:"compute_unit_margin_percent" validate:"max=100"`                             //nolint:lll
	ConfirmTimeout           time.Duration      `json:"confirm_timeout"             toml:"confirm_timeout"             validate:"gt=0"`                                //nolint:lll
	LoadedDataLimitBytes     uint32             `json:"loaded_data_limit_bytes"     toml:"loaded_data_limit_bytes"     validate:"gt=0"`                                //nolint:lll
	MaxPriorityFeeLamports   uint64             `json:"max_priority_fee_lamports"   toml:"max_priority_fee_lamports"`
	MaxTransactionBytes      int                `json:"max_transaction_bytes"       toml:"max_transaction_bytes"       validate:"gt=0"` //nolint:lll
	Paymaster                PaymasterConfig    `json:"paymaster"                   toml:"paymaster"`
	PriorityFeeLamports      uint64             `json:"priority_fee_lamports"       toml:"priority_fee_lamports"`
	PriorityFeeMode          string             `json:"priority_fee_mode"           toml:"priority_fee_mode"           validate:"oneof=auto fixed never"` //nolint:lll
	Retries                  uint               `json:"retries"                     toml:"retries"`
	RetryDelay               time.Duration      `json:"retry_delay"                 toml:"retry_delay"                 validate:"gte=0"` //nolint:lll
}

func (c Config) Validate() error {
	return validate.Struct(c)
}

type Option func(*options)

type PaymasterConfig struct {
	APIKey          string `json:"api_key"            toml:"api_key"`
	Fallback        bool   `json:"fallback"           toml:"fallback"`
	FeeToken        string `json:"fee_token"          toml:"fee_token"`
	MaxFeeBaseUnits uint64 `json:"max_fee_base_units" toml:"max_fee_base_units"`
	// URL is the kora JSON-RPC endpoint, under the key "endpoint". It was "url", which [rpc] url shares, so a
	// line edit of the RPC URL changed the paymaster too. LegacyURL still reads the old key, flag and env.
	URL       string `json:"endpoint" toml:"endpoint"      validate:"omitempty,url"`
	LegacyURL string `json:"-"        toml:"url,omitempty" validate:"omitempty,url"`
}

// ErrPaymasterURLConflict is returned when the endpoint and the deprecated url both hold different values.
var ErrPaymasterURLConflict = errors.New("tx.paymaster: endpoint and the deprecated url differ; set endpoint only")

// ResolveLegacy moves the deprecated url into the endpoint. Loaders call it after decoding.
func (c *PaymasterConfig) ResolveLegacy() error {
	if c.LegacyURL == "" {
		return nil
	}

	if c.URL != "" && c.URL != c.LegacyURL {
		return ErrPaymasterURLConflict
	}

	c.URL, c.LegacyURL = c.LegacyURL, ""

	return nil
}

type ProgramIDs struct {
	Distributor solana.PublicKey
	DVPN        solana.PublicKey
}

type Receipt struct {
	ComputeUnitLimit uint32           `json:"compute_unit_limit"`
	FeePayer         solana.PublicKey `json:"fee_payer"`
	Logs             []string         `json:"logs"`
	LogsSimulated    bool             `json:"logs_simulated"`
	Signature        solana.Signature `json:"signature"`
	Slot             uint64           `json:"slot"`
	TokenFee         *TokenFee        `json:"token_fee"`
}

type Sender struct {
	Config     Config
	DialWS     func(context.Context) (*ws.Client, error)
	Log        zerolog.Logger
	Paymaster  Paymaster
	ProgramIDs ProgramIDs
	RPC        *rpc.Client

	rentPayer atomic.Value
}

func NewSender(
	rpcClient *rpc.Client,
	dialWS func(context.Context) (*ws.Client, error),
	cfg Config,
	log zerolog.Logger,
) *Sender {
	return &Sender{
		Config: cfg,
		DialWS: dialWS,
		Log:    log,
		RPC:    rpcClient,
	}
}

func (s *Sender) RentPayer(ctx context.Context, signer solana.PublicKey) (solana.PublicKey, error) {
	rentPayer, err := resolvePayer(ctx, s.Paymaster, signer)
	if shouldFallBack(ctx, s.Paymaster, err) {
		s.Log.Warn().Err(err).Msg("paying rent with the signer")

		return signer, nil
	}

	if err != nil {
		return solana.PublicKey{}, err
	}

	s.rentPayer.Store(rentPayer)

	return rentPayer, nil
}

func (s *Sender) Send(
	ctx context.Context,
	signer solana.PrivateKey,
	ixs []solana.Instruction,
	opts ...Option,
) (*Receipt, error) {
	var settings options
	for _, opt := range opts {
		opt(&settings)
	}

	log := s.Log
	if settings.label != "" {
		log = log.With().Str("instruction", settings.label).Logger()
	}

	opts = append([]Option{withLog(log), withProgramIDs(s.ProgramIDs)}, opts...)

	if s.Paymaster != nil {
		rentPayer, _ := s.rentPayer.Load().(solana.PublicKey)
		opts = append([]Option{withPaymaster(s.Paymaster, rentPayer)}, opts...)
	}

	var wsClient *ws.Client

	if s.DialWS != nil {
		client, err := s.DialWS(ctx)
		if err != nil {
			s.Log.Warn().Err(err).Msg("websocket unavailable, confirming by signature-status polling alone")
		} else if client != nil {
			defer client.Close()

			wsClient = client
		}
	}

	receipt, err := Send(ctx, s.RPC, wsClient, signer, s.Config, ixs, opts...)
	if err != nil {
		if settings.label != "" {
			return nil, fmt.Errorf("%s: %w", settings.label, err)
		}

		return nil, err
	}

	log.Info().Str("signature", receipt.Signature.String()).Uint64("slot", receipt.Slot).Msg("transaction confirmed")

	return receipt, nil
}

type formattedRPCError struct {
	err    error
	rpcErr *jsonrpc.RPCError
}

func (e *formattedRPCError) Error() string {
	label := fmt.Sprintf("rpc error %d: %s", e.rpcErr.Code, e.rpcErr.Message)

	message := strings.Replace(e.err.Error(), e.rpcErr.Error(), label, 1)
	if message == e.err.Error() {
		return message + ": " + label
	}

	return message
}

func (e *formattedRPCError) Unwrap() error {
	return e.err
}

type options struct {
	computeUnitLimit uint32
	errorPrograms    ProgramIDs
	label            string
	log              zerolog.Logger
	paymaster        Paymaster
	rentPayer        solana.PublicKey
	signers          []solana.PrivateKey
}

type signedTx struct {
	computeUnitLimit     uint32
	errorPrograms        ProgramIDs
	lastValidBlockHeight uint64
	logs                 []string
	programs             []solana.PublicKey
	tokenFee             *TokenFee
	tx                   *solana.Transaction
}

func DefaultConfig() Config {
	return Config{
		Commitment:               rpc.CommitmentConfirmed,
		ComputeUnitMarginPercent: simulationComputeMarginPercent,
		ConfirmTimeout:           60 * time.Second,
		LoadedDataLimitBytes:     defaultLoadedDataLimitBytes,
		MaxPriorityFeeLamports:   100_000,
		MaxTransactionBytes:      program.DefaultMaxTransactionBytes,
		PriorityFeeMode:          PriorityFeeModeAuto,
		Retries:                  3,
		RetryDelay:               2 * time.Second,
	}
}

func EstimatePriorityFee(samples []rpc.PriorizationFeeResult, units uint32, maxFee uint64) uint64 {
	if len(samples) == 0 {
		return 0
	}

	recent := slices.Clone(samples)
	bySlotDesc := func(a, b rpc.PriorizationFeeResult) int {
		return cmp.Compare(b.Slot, a.Slot)
	}
	slices.SortFunc(recent, bySlotDesc)

	if len(recent) > feeWindowSlots {
		recent = recent[:feeWindowSlots]
	}

	fees := make([]uint64, 0, len(recent))
	for _, sample := range recent {
		fees = append(fees, sample.PrioritizationFee)
	}

	slices.Sort(fees)

	median := fees[(len(fees)-1)/2]
	if median < feeThresholdMicroLamports {
		return 0
	}

	high, low := bits.Mul64(median, uint64(units))

	sum, carry := bits.Add64(low, microLamportsPerLamport-1, 0)
	high += carry

	if high >= microLamportsPerLamport {
		return maxFee
	}

	total, _ := bits.Div64(high, sum, microLamportsPerLamport)

	return min(total, maxFee)
}

func Send(
	ctx context.Context,
	rpcClient *rpc.Client,
	wsClient *ws.Client,
	signer solana.PrivateKey,
	cfg Config,
	ixs []solana.Instruction,
	opts ...Option,
) (*Receipt, error) {
	if len(ixs) == 0 {
		return nil, &UnsentError{Err: ErrMissingInstructions}
	}

	settings := options{log: zerolog.Nop()}
	for _, opt := range opts {
		opt(&settings)
	}

	attemptedSend := false
	priorIndeterminate := false
	attempt := func() (*Receipt, error) {
		receipt, sent, err := sendOnce(ctx, rpcClient, wsClient, signer, cfg, ixs, settings)
		if !attemptedSend && !sent && shouldFallBack(ctx, settings.paymaster, err) {
			receipt, sent, err = sendSelfPaid(ctx, rpcClient, wsClient, signer, cfg, ixs, settings, err)
		}

		attemptedSend = attemptedSend || sent

		if failed, ok := errors.AsType[*FailedOnChainError](err); ok {
			failed.PriorIndeterminate = priorIndeterminate
		} else if sent && err != nil {
			priorIndeterminate = true
		}

		return receipt, err
	}
	retryable := func(err error) bool {
		return errors.Is(err, ErrBlockhashExpired)
	}

	receipt, err := retry.DoWithData(
		attempt,
		retry.Context(ctx),
		retry.Attempts(cfg.Retries+1),
		retry.Delay(cfg.RetryDelay),
		retry.RetryIf(retryable),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		if !attemptedSend {
			return nil, &UnsentError{Err: err}
		}

		return nil, err //nolint:wrapcheck
	}

	return receipt, nil
}

func WithComputeUnitLimit(units uint32) Option {
	return func(o *options) { o.computeUnitLimit = units }
}

func WithLabel(label string) Option {
	return func(o *options) { o.label = label }
}

func WithSigners(keys ...solana.PrivateKey) Option {
	return func(o *options) { o.signers = append(o.signers, keys...) }
}

func build(
	payer solana.PublicKey,
	ixs []solana.Instruction,
	blockhash solana.Hash,
	units uint32,
	fee uint64,
	loaded uint32,
	maxBytes int,
) (*solana.Transaction, error) {
	cfg := solana.TransactionConfig{}.WithComputeUnitLimit(units).WithLoadedAccountsDataSizeLimit(loaded)
	if fee != 0 {
		cfg = cfg.WithPriorityFee(fee)
	}

	t, err := solana.NewTransaction(ixs, blockhash, solana.TransactionPayer(payer), solana.TransactionV1Config(cfg))
	if err != nil {
		return nil, fmt.Errorf("building transaction: %w", err)
	}

	raw, err := t.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("serializing transaction: %w", err)
	}

	limit := min(maxBytes, solana.MaxTransactionSizeV1)
	if len(raw) > limit {
		return nil, fmt.Errorf("%w: %d bytes exceed %d", ErrTransactionTooLarge, len(raw), limit)
	}

	return t, nil
}

func classify(err error) error {
	if err == nil {
		return nil
	}

	if httpErr, ok := errors.AsType[*jsonrpc.HTTPError](err); ok &&
		(httpErr.Code == http.StatusTooManyRequests ||
			httpErr.Code >= http.StatusInternalServerError) {
		return fmt.Errorf("%w: %w", ErrRPCTransient, err)
	}

	if rpcErr, ok := errors.AsType[*jsonrpc.RPCError](err); ok {
		formatted := &formattedRPCError{
			err:    err,
			rpcErr: rpcErr,
		}
		switch {
		case strings.Contains(strings.ToLower(rpcErr.Message), "blockhash"):
			return fmt.Errorf("%w: %w", ErrBlockhashExpired, formatted)
		case rpcErr.Code == http.StatusTooManyRequests || rpcErr.Code == rpcNodeUnhealthy:
			return fmt.Errorf("%w: %w", ErrRPCTransient, formatted)
		}

		return formatted
	}

	if _, ok := errors.AsType[net.Error](err); ok || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %w", ErrRPCTransient, err)
	}

	return err
}

func collectPrograms(tx *solana.Transaction) []solana.PublicKey {
	programs := make([]solana.PublicKey, len(tx.Message.Instructions))
	for i, ix := range tx.Message.Instructions {
		programs[i] = tx.Message.AccountKeys[ix.ProgramIDIndex]
	}

	return programs
}

func prepare(
	ctx context.Context,
	rpcClient *rpc.Client,
	signer solana.PrivateKey,
	cfg Config,
	ixs []solana.Instruction,
	opts options,
) (*signedTx, error) {
	payer, err := resolvePayer(ctx, opts.paymaster, signer.PublicKey())
	if err != nil {
		return nil, err
	}

	feeTransfer, placeholders, err := resolveFeeTransfer(ctx, opts.paymaster, signer.PublicKey())
	if err != nil {
		return nil, err
	}

	ixs = slices.Concat(ixs, placeholders)

	blockhashOpts := rpc.GetLatestBlockhashOpts{Commitment: cfg.Commitment}

	hash, err := rpcClient.GetLatestBlockhashWithOpts(ctx, &blockhashOpts)
	if err != nil {
		return nil, fmt.Errorf("fetching latest blockhash: %w", classify(err))
	}

	probe, err := build(
		payer, ixs, hash.Value.Blockhash, maxComputeUnits, 0,
		cfg.LoadedDataLimitBytes, cfg.MaxTransactionBytes,
	)
	if err != nil {
		return nil, markOversizeFailed(opts.paymaster, err)
	}

	sim, units, err := simulate(ctx, rpcClient, cfg, probe, opts, len(ixs)-len(placeholders))
	if err != nil {
		return nil, err
	}

	fee, err := resolvePriorityFee(ctx, rpcClient, cfg, ixs, payer, units)
	if err != nil {
		return nil, err
	}

	buildWith := func(ixs []solana.Instruction) (*solana.Transaction, error) {
		t, err := build(payer, ixs, hash.Value.Blockhash, units, fee, cfg.LoadedDataLimitBytes, cfg.MaxTransactionBytes)

		return t, markOversizeFailed(opts.paymaster, err)
	}

	t, tokenFee, err := buildWithFeeTransfer(ctx, buildWith, ixs, feeTransfer, len(placeholders))
	if err != nil {
		return nil, err
	}

	keys := append([]solana.PrivateKey{signer}, opts.signers...)

	t, err = sign(ctx, t, keys, opts)
	if err != nil {
		return nil, err
	}

	signed := &signedTx{
		computeUnitLimit:     units,
		errorPrograms:        opts.errorPrograms,
		lastValidBlockHeight: hash.Value.LastValidBlockHeight,
		logs:                 sim.Logs,
		programs:             collectPrograms(t),
		tokenFee:             tokenFee,
		tx:                   t,
	}

	return signed, nil
}

func resolvePriorityFee(
	ctx context.Context,
	rpcClient *rpc.Client,
	cfg Config,
	ixs []solana.Instruction,
	payer solana.PublicKey,
	units uint32,
) (uint64, error) {
	switch cfg.PriorityFeeMode {
	case PriorityFeeModeFixed:
		return cfg.PriorityFeeLamports, nil
	case PriorityFeeModeAuto:
		samples, err := rpcClient.GetRecentPrioritizationFees(ctx, writable(ixs, payer))
		if err != nil {
			return 0, fmt.Errorf("fetching prioritization fees: %w", classify(err))
		}

		return EstimatePriorityFee(samples, units, cfg.MaxPriorityFeeLamports), nil
	case PriorityFeeModeNever:
		return 0, nil
	default:
		return 0, fmt.Errorf("%w: priority fee mode %q", validate.ErrInvalidConfig, cfg.PriorityFeeMode)
	}
}

func sendOnce(
	ctx context.Context,
	rpcClient *rpc.Client,
	wsClient *ws.Client,
	signer solana.PrivateKey,
	cfg Config,
	ixs []solana.Instruction,
	opts options,
) (receipt *Receipt, sent bool, err error) {
	signed, err := prepare(ctx, rpcClient, signer, cfg, ixs, opts)
	if err != nil {
		return nil, false, err
	}

	sendOpts := rpc.TransactionOpts{
		PreflightCommitment: cfg.Commitment,
		SkipPreflight:       true,
	}

	sig, err := rpcClient.SendTransactionWithOpts(ctx, signed.tx, sendOpts)
	if err != nil {
		return nil, true, fmt.Errorf("sending transaction: %w", classify(err))
	}

	slot, err := confirm(ctx, rpcClient, wsClient, signed, cfg)
	if err != nil {
		return nil, true, fmt.Errorf("sending transaction: %w", err)
	}

	logs, err := fetchLandedLogs(ctx, rpcClient, sig, cfg.Commitment)

	logsSimulated := err != nil
	if logsSimulated {
		logs = signed.logs
	}

	receipt = &Receipt{
		ComputeUnitLimit: signed.computeUnitLimit,
		FeePayer:         signed.tx.Message.AccountKeys[0],
		Logs:             logs,
		LogsSimulated:    logsSimulated,
		Signature:        sig,
		Slot:             slot,
		TokenFee:         signed.tokenFee,
	}

	return receipt, true, nil
}

func sign(
	ctx context.Context,
	t *solana.Transaction,
	keys []solana.PrivateKey,
	opts options,
) (*solana.Transaction, error) {
	lookup := func(pub solana.PublicKey) *solana.PrivateKey {
		for i := range keys {
			if keys[i].PublicKey() == pub {
				return &keys[i]
			}
		}

		return nil
	}

	if opts.paymaster != nil {
		return signWithPaymaster(ctx, t, lookup, opts)
	}

	if _, err := t.Sign(lookup); err != nil {
		return nil, fmt.Errorf("signing: %w", err)
	}

	return t, nil
}

func simulate(
	ctx context.Context,
	rpcClient *rpc.Client,
	cfg Config,
	probe *solana.Transaction,
	opts options,
	firstFeeTransfer int,
) (*rpc.SimulateTransactionResult, uint32, error) {
	simOpts := rpc.SimulateTransactionOpts{
		Commitment:             cfg.Commitment,
		ReplaceRecentBlockhash: true,
	}

	sim, err := rpcClient.SimulateTransactionWithOpts(ctx, probe, &simOpts)
	if err != nil {
		return nil, 0, fmt.Errorf("simulating: %w", classify(err))
	}

	if sim.Value.Err != nil {
		simErr := fmt.Errorf("simulation failed: %v", sim.Value.Err)
		if decoded := core.DecodeTxError(sim.Value.Err, sim.Value.Logs, collectPrograms(probe)); decoded != nil {
			simErr = fmt.Errorf("simulation failed: %w", programError(decoded, opts.errorPrograms))
		}

		if blamesPaymaster(sim.Value.Err, probe, firstFeeTransfer, opts.rentPayer) {
			return nil, 0, markPaymasterFailed(opts.paymaster, simErr)
		}

		return nil, 0, simErr
	}

	units := opts.computeUnitLimit
	if units == 0 {
		consumed := uint64(minComputeUnits)
		if sim.Value.UnitsConsumed != nil && *sim.Value.UnitsConsumed > consumed {
			consumed = *sim.Value.UnitsConsumed
		}

		units = uint32(min(consumed+consumed*uint64(cfg.ComputeUnitMarginPercent)/percent, maxComputeUnits))
	}

	return sim.Value, units, nil
}

func withLog(log zerolog.Logger) Option {
	return func(o *options) { o.log = log }
}

func withProgramIDs(ids ProgramIDs) Option {
	return func(o *options) { o.errorPrograms = ids }
}

func writable(ixs []solana.Instruction, payer solana.PublicKey) solana.PublicKeySlice {
	out := solana.PublicKeySlice{payer}

	for _, instruction := range ixs {
		for _, meta := range instruction.Accounts() {
			if meta.IsWritable {
				out.UniqueAppend(meta.PublicKey)
			}
		}
	}

	return out
}
