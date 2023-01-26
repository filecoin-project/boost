package retrieve

import "fmt"

type ErrorCode int

const (
	ErrUnknown ErrorCode = iota

	// Failed to connect to a miner.
	ErrMinerConnectionFailed

	// There was an issue related to the Lotus API.
	ErrLotusError
)

type Error struct {
	Code  ErrorCode
	Inner error
}

func (code ErrorCode) String() string {
	switch code {
	case ErrUnknown:
		return "unknown"
	case ErrMinerConnectionFailed:
		return "miner connection failed"
	case ErrLotusError:
		return "lotus error"
	default:
		return "(invalid error code)"
	}
}

func (err *Error) Error() string {
	return fmt.Sprintf("%s: %s", err.Code, err.Inner)
}

func (err *Error) Unwrap() error {
	return err.Inner
}

func NewError(code ErrorCode, err error) *Error {
	return &Error{
		Code:  code,
		Inner: err,
	}
}

func NewErrUnknown(err error) error {
	return NewError(ErrUnknown, err)
}

func NewErrMinerConnectionFailed(err error) error {
	return NewError(ErrMinerConnectionFailed, err)
}

func NewErrLotusError(err error) error {
	return NewError(ErrLotusError, err)
}
