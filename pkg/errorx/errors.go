// Copyright 2021-2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errorx

import (
	"io"
	"net/url"
	"strings"
)

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

type ErrorWithCode interface {
	Error() string
	Code() ErrorCode
}

func IsRecoverAbleError(err error) bool {
	if strings.Contains(err.Error(), "connection reset by peer") || strings.Contains(err.Error(), "No connection could be made") {
		return true
	}
	if urlErr, ok := err.(*url.Error); ok {
		// consider timeout and temporary error as recoverable
		if urlErr.Timeout() || urlErr.Temporary() || urlErr.Err == io.EOF {
			return true
		}
	}
	return false
}

type MockTemporaryError struct{}

func (e *MockTemporaryError) Error() string {
	return "mockTimeoutError"
}

func (e *MockTemporaryError) Temporary() bool { return true }
