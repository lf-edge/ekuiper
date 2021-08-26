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

package kv

type Tskv interface {
	Set(k int64, v interface{}) (inserted bool, err error)
	Get(k int64, v interface{}) (found bool, err error)
	Last(v interface{}) (key int64, err error)
	Delete(k int64) error
	DeleteBefore(int64) error
	Close() error
	Drop() error
}
