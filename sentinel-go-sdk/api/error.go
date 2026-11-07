package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/sentinel-official/sentinel-go-sdk/v3/auth"
	"github.com/sentinel-official/sentinel-programs/clients/go/core"
)

const (
	CodeBadRequest              = "bad_request"
	CodeChannelNodeStale        = "channel_node_stale"
	CodeChannelNotOpen          = "channel_not_open"
	CodeClientClosedRequest     = "client_closed_request"
	CodeConflict                = "conflict"
	CodeInternal                = "internal"
	CodeInvalidSubscription     = "invalid_subscription"
	CodeInvalidTicket           = "invalid_ticket"
	CodeInvalidVoucherSignature = "invalid_voucher_signature"
	CodeLeaseNotFunded          = "lease_not_funded"
	CodeMethodNotAllowed        = "method_not_allowed"
	CodeModeNotStream           = "mode_not_stream"
	CodeModeNotVoucher          = "mode_not_voucher"
	CodeNotFound                = "not_found"
	CodePeerLimitReached        = "peer_limit_reached"
	CodePrepayBelowMinimum      = "prepay_below_minimum"
	CodeRateLimited             = "rate_limited"
	CodeRequestTooLarge         = "request_too_large"
	CodeSessionNotFound         = "session_not_found"
	CodeTermsRejected           = "terms_rejected"
	CodeUnauthorized            = "unauthorized"
	CodeVoucherExceedsEscrow    = "voucher_exceeds_escrow"
)

const MaxErrorMessageBytes = 256

const statusClientClosedRequest = 499

var (
	ErrBadRequest = &Error{
		Code:       CodeBadRequest,
		HTTPStatus: http.StatusBadRequest,
		Message:    "bad request",
	}
	ErrChannelNodeStale = &Error{
		Code:       CodeChannelNodeStale,
		HTTPStatus: http.StatusConflict,
		Message:    "channel was opened against a previous node incarnation",
	}
	ErrChannelNotOpen = &Error{
		Code:       CodeChannelNotOpen,
		HTTPStatus: http.StatusNotFound,
		Message:    "channel is not open",
	}
	ErrClientClosedRequest = &Error{
		Code:       CodeClientClosedRequest,
		HTTPStatus: statusClientClosedRequest,
		Message:    "client closed request",
	}
	ErrConflict = &Error{
		Code:       CodeConflict,
		HTTPStatus: http.StatusConflict,
		Message:    "resource conflict",
	}
	ErrInternal = &Error{
		Code:       CodeInternal,
		HTTPStatus: http.StatusInternalServerError,
		Message:    "internal error",
	}
	ErrInvalidSubscription = &Error{
		Code:       CodeInvalidSubscription,
		HTTPStatus: http.StatusForbidden,
		Message:    "subscription is not valid for this node",
	}
	ErrInvalidTicket = &Error{
		Code:       CodeInvalidTicket,
		HTTPStatus: http.StatusForbidden,
		Message:    "ticket is not valid",
	}
	ErrInvalidVoucherSignature = &Error{
		Code:       CodeInvalidVoucherSignature,
		HTTPStatus: http.StatusBadRequest,
		Message:    "voucher signature is invalid",
	}
	ErrLeaseNotFunded = &Error{
		Code:       CodeLeaseNotFunded,
		HTTPStatus: http.StatusPaymentRequired,
		Message:    "lease is not funded for this session",
	}
	ErrMethodNotAllowed = &Error{
		Code:       CodeMethodNotAllowed,
		HTTPStatus: http.StatusMethodNotAllowed,
		Message:    "method not allowed",
	}
	ErrModeNotStream = &Error{
		Code:       CodeModeNotStream,
		HTTPStatus: http.StatusBadRequest,
		Message:    "channel is not in stream mode",
	}
	ErrModeNotVoucher = &Error{
		Code:       CodeModeNotVoucher,
		HTTPStatus: http.StatusBadRequest,
		Message:    "channel is not in voucher mode",
	}
	ErrNotFound = &Error{
		Code:       CodeNotFound,
		HTTPStatus: http.StatusNotFound,
		Message:    "resource not found",
	}
	ErrPeerLimitReached = &Error{
		Code:       CodePeerLimitReached,
		HTTPStatus: http.StatusConflict,
		Message:    "peer limit reached",
	}
	ErrPrepayBelowMinimum = &Error{
		Code:       CodePrepayBelowMinimum,
		HTTPStatus: http.StatusPaymentRequired,
		Message:    "prepay below node minimum",
	}
	ErrRateLimited = &Error{
		Code:       CodeRateLimited,
		HTTPStatus: http.StatusTooManyRequests,
		Message:    "rate limited",
	}
	ErrRequestTooLarge = &Error{
		Code:       CodeRequestTooLarge,
		HTTPStatus: http.StatusRequestEntityTooLarge,
		Message:    "request too large",
	}
	ErrSessionNotFound = &Error{
		Code:       CodeSessionNotFound,
		HTTPStatus: http.StatusNotFound,
		Message:    "session not found",
	}
	ErrTermsRejected = &Error{
		Code:       CodeTermsRejected,
		HTTPStatus: http.StatusBadRequest,
		Message:    "terms rejected",
	}
	ErrUnauthorized = &Error{
		Code:       CodeUnauthorized,
		HTTPStatus: http.StatusUnauthorized,
		Message:    "unauthorized",
	}
	ErrUnexpectedStatus     = errors.New("unexpected status")
	ErrVoucherExceedsEscrow = &Error{
		Code:       CodeVoucherExceedsEscrow,
		HTTPStatus: http.StatusBadRequest,
		Message:    "voucher exceeds escrow",
	}
)

type Error struct {
	Code       string `json:"code"`
	HTTPStatus int    `json:"-"`
	Message    string `json:"message"`
}

func (e *Error) Error() string {
	return e.Code + ": " + e.Message
}

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)

	return ok && t.Code == e.Code
}

type ErrorResponse struct {
	Error Error `json:"error"`
}

func Abort(ginCtx *gin.Context, err error) {
	apiErr := classify(err)
	if apiErr == ErrInternal && err != nil {
		_ = ginCtx.Error(err)
	}

	body := ErrorResponse{Error: *apiErr}

	ginCtx.AbortWithStatusJSON(apiErr.HTTPStatus, body)
}

func AbortRequest(ginCtx *gin.Context, err error) {
	apiErr := classify(err)
	if apiErr == ErrInternal {
		apiErr = ErrBadRequest
	}

	Abort(ginCtx, apiErr)
}

func Codes() []string {
	tbl := errorTable()

	out := make([]string, 0, len(tbl))
	for _, apiErr := range tbl {
		out = append(out, apiErr.Code)
	}

	return out
}

func ParseError(status int, body []byte) error {
	var response ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil ||
		!codePattern.MatchString(response.Error.Code) ||
		!validMessage(response.Error.Message) {
		return fmt.Errorf("%w %d", ErrUnexpectedStatus, status)
	}

	return &Error{
		Code:       response.Error.Code,
		HTTPStatus: status,
		Message:    response.Error.Message,
	}
}

func classify(err error) *Error {
	if apiErr, ok := errors.AsType[*Error](err); ok {
		return apiErr
	}

	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return ErrRequestTooLarge
	}

	var (
		syntaxErr *json.SyntaxError
		typeErr   *json.UnmarshalTypeError
	)

	switch {
	case errors.Is(err, context.Canceled):
		return ErrClientClosedRequest
	case errors.As(err, &syntaxErr), errors.As(err, &typeErr), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return ErrBadRequest
	case errors.Is(err, auth.ErrUnauthorized), errors.Is(err, auth.ErrTimestampOutOfRange),
		errors.Is(err, auth.ErrNonceReplayed), errors.Is(err, auth.ErrInvalidSignature),
		errors.Is(err, auth.ErrInvalidSignatureHeader):
		return ErrUnauthorized
	case errors.Is(err, auth.ErrTicketExpired), errors.Is(err, auth.ErrTicketNodeMismatch),
		errors.Is(err, auth.ErrTicketRequestStale), errors.Is(err, auth.ErrUnsupportedTicketVersion):
		return ErrInvalidTicket
	case errors.Is(err, core.ErrAccountNotFound), errors.Is(err, core.ErrOwnerMismatch):
		return ErrChannelNotOpen
	default:
		return ErrInternal
	}
}

func errorTable() []*Error {
	return []*Error{
		ErrBadRequest, ErrChannelNodeStale, ErrChannelNotOpen, ErrClientClosedRequest, ErrConflict, ErrInternal,
		ErrInvalidSubscription, ErrInvalidTicket, ErrInvalidVoucherSignature, ErrLeaseNotFunded, ErrMethodNotAllowed,
		ErrModeNotStream, ErrModeNotVoucher, ErrNotFound, ErrPeerLimitReached, ErrPrepayBelowMinimum,
		ErrRateLimited, ErrRequestTooLarge, ErrSessionNotFound,
		ErrTermsRejected, ErrUnauthorized, ErrVoucherExceedsEscrow,
	}
}

func validMessage(message string) bool {
	if len(message) > MaxErrorMessageBytes {
		return false
	}

	for i := range len(message) {
		if message[i] < 0x20 || message[i] > 0x7e {
			return false
		}
	}

	return true
}
