// Copyright 2024 EMQ Technologies Co., Ltd.
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

type ErrorCode int

const (
	Undefined_Err ErrorCode = 1000
	GENERAL_ERR   ErrorCode = 1001
	NOT_FOUND     ErrorCode = 1002
	IOErr         ErrorCode = 1003

	ParserError = 2001

	PlanError = 3001

	ExecutorError = 4001
)

var NotFoundErr = newWithCode(NOT_FOUND, "not found")

func NewIOErr(msg string) error {
	return &Error{
		code: IOErr,
		msg:  msg,
	}
}

func IsIOError(err error) bool {
	if withCode, ok := err.(ErrorWithCode); ok {
		return withCode.Code() == IOErr
	}
	return false
}

func NewParserError(msg string) error {
	return &Error{
		code: ParserError,
		msg:  msg,
	}
}

func GetErrorCode(err error) (ErrorCode, bool) {
	if code, ok := err.(ErrorWithCode); ok {
		return code.Code(), true
	}
	return 0, false
}
