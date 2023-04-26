// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	GENERAL_ERR ErrorCode = iota
	NOT_FOUND
)

const IOErr = "io error"

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
