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
	GENERAL_ERR ErrorCode = 1001
	NOT_FOUND   ErrorCode = 1002
	IOErr       ErrorCode = 1003
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
