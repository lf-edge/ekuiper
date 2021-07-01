package errorx

type ErrorCode int

const (
	GENERAL_ERR ErrorCode = iota
	NOT_FOUND
)

var NotFoundErr = NewWithCode(NOT_FOUND, "not found")

type Error struct {
	msg  string
	code ErrorCode
}

func New(message string) *Error {
	return &Error{message, GENERAL_ERR}
}

func NewWithCode(code ErrorCode, message string) *Error {
	return &Error{message, code}
}

func (e *Error) Error() string {
	return e.msg
}

func (e *Error) Code() ErrorCode {
	return e.code
}
