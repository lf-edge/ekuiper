// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package xsql

import "time"

type ErrorSourceTuple struct {
	Error error `json:"error"`
}

func (t *ErrorSourceTuple) Message() map[string]interface{} {
	return nil
}

func (t *ErrorSourceTuple) Meta() map[string]interface{} {
	return nil
}

func (t *ErrorSourceTuple) Timestamp() time.Time {
	return time.Now()
}
