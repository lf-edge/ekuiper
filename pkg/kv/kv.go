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

package kv

type KeyValue interface {
	// Setnx sets key to hold string value if key does not exist otherwise return an error
	Setnx(key string, value interface{}) error
	// Set key to hold the string value. If key already holds a value, it is overwritten
	Set(key string, value interface{}) error
	Get(key string, val interface{}) (bool, error)
	GetKeyedState(key string) (interface{}, error)
	SetKeyedState(key string, value interface{}) error
	// Delete must return *common.Error with NOT_FOUND error
	Delete(key string) error
	Keys() (keys []string, err error)
	All() (all map[string]string, err error)
	Clean() error
	Drop() error
}
