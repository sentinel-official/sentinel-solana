package sdktest

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/rpc"

	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// FeeLamports is the fee the fake quotes for any message (one signature at the base rate).
const FeeLamports = 5_000

type Account struct {
	Data     []byte
	Lamports uint64
	Owner    solana.PublicKey
}

type FakeRPC struct {
	Blockhash                 solana.Hash
	BlockHeight               uint64
	LandedErr                 any
	Logs                      []string
	PrioritizationFees        []uint64
	Server                    *httptest.Server
	Simulate                  func(tx *solana.Transaction) SimulateResult
	Slot                      uint64
	StatusErrors              int
	Statuses                  map[solana.Signature]string
	TransactionMetaErrMissing bool
	TransactionMetaMissing    bool
	TransactionMissing        bool
	TransactionRPCError       bool
	Unconfirmed               bool

	accountInfoCommitment     rpc.CommitmentType
	accounts                  map[solana.PublicKey]Account
	mu                        sync.Mutex
	programAccountsCommitment rpc.CommitmentType
	sent                      []*solana.Transaction
}

func NewFakeRPC(t *testing.T) *FakeRPC {
	t.Helper()

	fakeRPC := &FakeRPC{
		Blockhash: solana.MustHashFromBase58("EkSnNWid2cvwEVnVx9aBqawnmiCNiDgp3gUdkDPTKN1N"),
		Slot:      100,
		Statuses:  map[solana.Signature]string{},
		accounts:  map[solana.PublicKey]Account{},
	}
	fakeRPC.Server = httptest.NewServer(fakeRPC)
	t.Cleanup(fakeRPC.Server.Close)

	return fakeRPC
}

func (f *FakeRPC) AccountInfoCommitment() rpc.CommitmentType {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.accountInfoCommitment
}

func (f *FakeRPC) ClientWithCommitment(commitment rpc.CommitmentType) *rpc.Client {
	return rpc.NewWithCommitment(f.Server.URL, commitment)
}

func (f *FakeRPC) Config() cli.RPCConfig {
	cfg := cli.DefaultRPCConfig(cli.NetworkLocalnet)
	cfg.URL = f.Server.URL
	cfg.WSURL = "ws" + strings.TrimPrefix(f.Server.URL, "http")

	return cfg
}

func (f *FakeRPC) NewClient() *rpc.Client {
	return rpc.New(f.Server.URL)
}

func (f *FakeRPC) ProgramAccountsCommitment() rpc.CommitmentType {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.programAccountsCommitment
}

func (f *FakeRPC) Sent() []*solana.Transaction {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.sent)
}

func (f *FakeRPC) ServeHTTP(writer http.ResponseWriter, httpReq *http.Request) {
	var req request
	if err := json.NewDecoder(httpReq.Body).Decode(&req); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)

		return
	}

	f.mu.Lock()
	if req.Method == "getSignatureStatuses" && f.StatusErrors > 0 {
		f.StatusErrors--
		f.mu.Unlock()
		http.Error(writer, "status unavailable", http.StatusInternalServerError)

		return
	}

	result, rpcErr := f.dispatch(req)
	f.mu.Unlock()

	resp := response{
		ID:      req.ID,
		JSONRPC: "2.0",
		Result:  result,
		Error:   rpcErr,
	}

	writer.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(writer).Encode(resp); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (f *FakeRPC) SetAccount(address solana.PublicKey, account Account) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.accounts[address] = account
}

func (f *FakeRPC) SetConfig(programID solana.PublicKey, cfg dvpn.Config) error {
	address, _, err := dvpn.FindConfigAddress(programID)
	if err != nil {
		return fmt.Errorf("deriving config address: %w", err)
	}

	account := Account{
		Data:  EncodeAccount(dvpn.ConfigAccountDiscriminator, cfg),
		Owner: programID,
	}
	f.SetAccount(address, account)

	return nil
}

func (f *FakeRPC) SetGrant(programID, granter, grantee, target solana.PublicKey, permissions uint64) error {
	address, _, err := dvpn.FindGrantAddress(programID, granter, grantee, target)
	if err != nil {
		return fmt.Errorf("deriving grant address: %w", err)
	}

	grant := dvpn.Grant{
		Allowance:   dvpn.AllowanceUnlimited,
		Grantee:     grantee,
		Granter:     granter,
		Permissions: permissions,
		Target:      target,
	}
	account := Account{
		Data:  EncodeAccount(dvpn.GrantAccountDiscriminator, grant),
		Owner: programID,
	}
	f.SetAccount(address, account)

	return nil
}

func (f *FakeRPC) SetNode(programID, address solana.PublicKey, node dvpn.Node) {
	account := Account{
		Data:  EncodeAccount(dvpn.NodeAccountDiscriminator, node),
		Owner: programID,
	}
	f.SetAccount(address, account)
}

func (f *FakeRPC) SetStatus(sig solana.Signature, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.Statuses[sig] = status
}

func (f *FakeRPC) accountJSON(account Account) map[string]any {
	return map[string]any{
		"data":       []string{base64.StdEncoding.EncodeToString(account.Data), "base64"},
		"executable": false,
		"lamports":   account.Lamports,
		"owner":      account.Owner.String(),
		"rentEpoch":  0,
		"space":      len(account.Data),
	}
}

func (f *FakeRPC) context() map[string]any {
	return map[string]any{"slot": f.Slot}
}

func (f *FakeRPC) dispatch(req request) (any, *rpcError) {
	switch req.Method {
	case "getAccountInfo":
		return f.getAccountInfo(req.Params)
	case "getMultipleAccounts":
		return f.getMultipleAccounts(req.Params)
	case "getProgramAccounts":
		return f.getProgramAccounts(req.Params)
	case "getLatestBlockhash":
		value := map[string]any{"blockhash": f.Blockhash.String(), "lastValidBlockHeight": f.Slot + 150}

		return map[string]any{"context": f.context(), "value": value}, nil
	case "getRecentPrioritizationFees":
		out := make([]map[string]any, 0, len(f.PrioritizationFees))
		for i, fee := range f.PrioritizationFees {
			slot := f.Slot - uint64(len(f.PrioritizationFees)) + uint64(i)
			out = append(out, map[string]any{"prioritizationFee": fee, "slot": slot})
		}

		return out, nil
	case "getSlot":
		return f.Slot, nil
	case "getBlockHeight":
		return f.BlockHeight, nil
	case "getBlockTime":
		return time.Now().Unix(), nil
	case "getHealth":
		return "ok", nil
	case "getFeeForMessage":
		return map[string]any{"context": f.context(), "value": FeeLamports}, nil
	case "sendTransaction":
		return f.sendTransaction(req.Params)
	case "simulateTransaction":
		return f.simulateTransaction(req.Params)
	case "getSignatureStatuses":
		return f.getSignatureStatuses(req.Params)
	case "getTransaction":
		return f.getTransaction(req.Params)
	default:
		return nil, &rpcError{
			Code:    -32601,
			Message: "method not found: " + req.Method,
		}
	}
}

func (f *FakeRPC) getAccountInfo(params []json.RawMessage) (any, *rpcError) {
	var address solana.PublicKey
	if err := json.Unmarshal(params[0], &address); err != nil {
		return nil, &rpcError{
			Code:    -32602,
			Message: err.Error(),
		}
	}

	var opts rpc.GetAccountInfoOpts
	if len(params) > 1 {
		if err := json.Unmarshal(params[1], &opts); err != nil {
			return nil, &rpcError{
				Code:    -32602,
				Message: err.Error(),
			}
		}
	}

	f.accountInfoCommitment = opts.Commitment

	account, ok := f.accounts[address]
	if !ok {
		return map[string]any{"context": f.context(), "value": nil}, nil
	}

	return map[string]any{"context": f.context(), "value": f.accountJSON(account)}, nil
}

func (f *FakeRPC) getMultipleAccounts(params []json.RawMessage) (any, *rpcError) {
	var addresses []solana.PublicKey
	if err := json.Unmarshal(params[0], &addresses); err != nil {
		return nil, &rpcError{
			Code:    -32602,
			Message: err.Error(),
		}
	}

	values := make([]any, 0, len(addresses))
	for _, address := range addresses {
		if account, ok := f.accounts[address]; ok {
			values = append(values, f.accountJSON(account))
		} else {
			values = append(values, nil)
		}
	}

	return map[string]any{"context": f.context(), "value": values}, nil
}

func (f *FakeRPC) getProgramAccounts(params []json.RawMessage) (any, *rpcError) {
	var program solana.PublicKey
	if err := json.Unmarshal(params[0], &program); err != nil {
		return nil, &rpcError{
			Code:    -32602,
			Message: err.Error(),
		}
	}

	var opts rpc.GetProgramAccountsOpts
	if len(params) > 1 {
		if err := json.Unmarshal(params[1], &opts); err != nil {
			return nil, &rpcError{
				Code:    -32602,
				Message: err.Error(),
			}
		}
	}

	f.programAccountsCommitment = opts.Commitment

	out := []map[string]any{}

	for address, account := range f.accounts {
		if account.Owner != program || !matches(account.Data, opts.Filters) {
			continue
		}

		out = append(out, map[string]any{"account": f.accountJSON(account), "pubkey": address.String()})
	}

	byAddress := func(a, b map[string]any) int {
		return cmp.Compare(a["pubkey"].(string), b["pubkey"].(string))
	}
	slices.SortFunc(out, byAddress)

	return map[string]any{"context": f.context(), "value": out}, nil
}

func (f *FakeRPC) getSignatureStatuses(params []json.RawMessage) (any, *rpcError) {
	var sigs []solana.Signature
	if err := json.Unmarshal(params[0], &sigs); err != nil {
		return nil, &rpcError{
			Code:    -32602,
			Message: err.Error(),
		}
	}

	values := make([]any, 0, len(sigs))
	for _, sig := range sigs {
		status, ok := f.Statuses[sig]
		if !ok || status == "" {
			values = append(values, nil)

			continue
		}

		values = append(values, map[string]any{
			"confirmationStatus": status,
			"confirmations":      1,
			"err":                f.LandedErr,
			"slot":               f.Slot,
		})
	}

	return map[string]any{"context": f.context(), "value": values}, nil
}

func (f *FakeRPC) getTransaction(params []json.RawMessage) (any, *rpcError) {
	var opts rpc.GetTransactionOpts

	if len(params) > 1 {
		if err := json.Unmarshal(params[1], &opts); err != nil {
			return nil, &rpcError{
				Code:    -32602,
				Message: err.Error(),
			}
		}
	}

	if opts.MaxSupportedTransactionVersion == nil ||
		*opts.MaxSupportedTransactionVersion != rpc.MaxSupportedTransactionVersion1 {
		return nil, &rpcError{
			Code:    -32015,
			Message: "Transaction version (1) is not supported by the requesting client",
		}
	}

	if opts.Commitment == rpc.CommitmentProcessed {
		return nil, &rpcError{
			Code:    -32602,
			Message: "processed commitment is not supported by getTransaction",
		}
	}

	if f.TransactionRPCError {
		return nil, &rpcError{
			Code:    -32000,
			Message: "transaction unavailable",
		}
	}

	if f.TransactionMissing {
		return nil, nil
	}

	if f.TransactionMetaMissing {
		return map[string]any{"meta": nil, "slot": f.Slot}, nil
	}

	metaErr := f.LandedErr
	if f.TransactionMetaErrMissing {
		metaErr = nil
	}

	meta := map[string]any{"err": metaErr, "logMessages": f.Logs}

	return map[string]any{"meta": meta, "slot": f.Slot}, nil
}

func (f *FakeRPC) sendTransaction(params []json.RawMessage) (any, *rpcError) {
	transaction, err := decodeTx(params[0])
	if err != nil {
		return nil, &rpcError{
			Code:    -32602,
			Message: err.Error(),
		}
	}

	f.sent = append(f.sent, transaction)
	if !f.Unconfirmed {
		f.Statuses[transaction.Signatures[0]] = "confirmed"
	}

	return transaction.Signatures[0].String(), nil
}

func (f *FakeRPC) simulateTransaction(params []json.RawMessage) (any, *rpcError) {
	transaction, err := decodeTx(params[0])
	if err != nil {
		return nil, &rpcError{
			Code:    -32602,
			Message: err.Error(),
		}
	}

	result := SimulateResult{UnitsConsumed: 5_000}
	if f.Simulate != nil {
		result = f.Simulate(transaction)
	}

	value := map[string]any{"err": result.Err, "logs": result.Logs, "unitsConsumed": result.UnitsConsumed}

	return map[string]any{"context": f.context(), "value": value}, nil
}

type SimulateResult struct {
	Err           any
	Logs          []string
	UnitsConsumed uint64
}

type request struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

type response struct {
	Error   *rpcError       `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
	JSONRPC string          `json:"jsonrpc"`
	Result  any             `json:"result,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func decodeTx(raw json.RawMessage) (*solana.Transaction, error) {
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return nil, fmt.Errorf("decoding transaction param: %w", err)
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding transaction base64: %w", err)
	}

	transaction, err := solana.TransactionFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("parsing transaction: %w", err)
	}

	return transaction, nil
}

func matches(data []byte, filters []rpc.RPCFilter) bool {
	for _, filter := range filters {
		if filter.DataSize != 0 && uint64(len(data)) != filter.DataSize {
			return false
		}

		if filter.Memcmp == nil {
			continue
		}

		end := int(filter.Memcmp.Offset) + len(filter.Memcmp.Bytes)
		if end > len(data) || !bytes.Equal(data[filter.Memcmp.Offset:end], []byte(filter.Memcmp.Bytes)) {
			return false
		}
	}

	return true
}
