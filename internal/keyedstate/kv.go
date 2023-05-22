// Copyright 2023 EMQ Technologies Co., Ltd.
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

package keyedstate

import (
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	kv2 "github.com/lf-edge/ekuiper/pkg/kv"
)

var kv kv2.KeyValue

type Manager struct {
	kv kv2.KeyValue
}

func InitKeyedStateKV() {
	kv, _ = store.GetExtStateKV("keyed_state")
}

func GetKeyedState(key string) (interface{}, error) {
	return kv.GetKeyedState(key)
}

func SetKeyedState(key string, value interface{}) error {
	return kv.SetKeyedState(key, value)
}

func ClearKeyedState() error {
	return kv.Drop()
}
