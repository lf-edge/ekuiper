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

var manager *Manager

type Manager struct {
	keyPrefix    string
	keySeparator string
	kv           kv2.KeyValue
}

func InitManager(keyPrefix, keySeparator string) {
	kv, _ := store.GetKV(keyPrefix)
	manager = &Manager{
		keyPrefix:    keyPrefix,
		keySeparator: keySeparator,
		kv:           kv,
	}
	return
}

func GetKeyedState(groupName string, keys []string) map[string]interface{} {
	result := map[string]interface{}{}
	for _, key := range keys {
		redisKey := manager.keySeparator + groupName + manager.keySeparator + key
		value, err := manager.kv.GetKeyedState(redisKey)
		if err != nil {
			result[key] = err.Error()
		} else {
			result[key] = value
		}
	}
	return result
}
