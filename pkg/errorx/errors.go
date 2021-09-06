// Copyright 2021 EMQ Technologies Co., Ltd.
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

import "fmt"

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

type MultiError map[string]error

func (e MultiError) Error() string {
	var s string
	switch len(e) {
	case 0, 1:
		s = ""
	default:
		s = "Get multiple errors: "
	}
	for k, v := range e {
		s = fmt.Sprintf("%s\n%s:%s", s, k, v.Error())
	}
	return s
}

func (e MultiError) GetError() error {
	if len(e) > 0 {
		return e
	}
	return nil
}
