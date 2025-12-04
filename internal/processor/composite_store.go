// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package processor

import (
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

// compositeStore is a wrapper that checks both persistent and temp stores.
// It implements kv.KeyValue interface.
type compositeStore struct {
	primary  kv.KeyValue
	fallback kv.KeyValue
}

func newCompositeStore(primary, fallback kv.KeyValue) *compositeStore {
	return &compositeStore{
		primary:  primary,
		fallback: fallback,
	}
}

func (c *compositeStore) Open() error {
	// Both stores are already open
	return nil
}

func (c *compositeStore) Close() error {
	// Don't close the underlying stores as they're managed elsewhere
	return nil
}

func (c *compositeStore) Set(key string, value interface{}) error {
	// Only set in primary store
	return c.primary.Set(key, value)
}

func (c *compositeStore) Setnx(key string, value interface{}) error {
	// Check both stores for existence
	var v string
	ok, _ := c.primary.Get(key, &v)
	if ok {
		return c.primary.Setnx(key, value)
	}
	ok, _ = c.fallback.Get(key, &v)
	if ok {
		return c.fallback.Setnx(key, value)
	}
	return c.primary.Setnx(key, value)
}

func (c *compositeStore) Get(key string, value interface{}) (bool, error) {
	// Try primary first
	ok, err := c.primary.Get(key, value)
	if ok || err != nil {
		return ok, err
	}
	// Try fallback
	return c.fallback.Get(key, value)
}

func (c *compositeStore) Delete(key string) error {
	// Try to delete from both
	err := c.primary.Delete(key)
	if err != nil {
		return c.fallback.Delete(key)
	}
	return nil
}

func (c *compositeStore) Keys() ([]string, error) {
	// Get keys from both stores
	keys1, err := c.primary.Keys()
	if err != nil {
		return nil, err
	}
	keys2, err := c.fallback.Keys()
	if err != nil {
		return keys1, nil
	}
	return append(keys1, keys2...), nil
}

func (c *compositeStore) All() (map[string]string, error) {
	// Get all from both stores
	all1, err := c.primary.All()
	if err != nil {
		return nil, err
	}
	all2, err := c.fallback.All()
	if err != nil {
		return all1, nil
	}
	// Merge maps
	for k, v := range all2 {
		all1[k] = v
	}
	return all1, nil
}

func (c *compositeStore) Clean() error {
	// Only clean primary
	return c.primary.Clean()
}

func (c *compositeStore) Drop() error {
	// Only drop primary
	return c.primary.Drop()
}

func (c *compositeStore) SetKeyedState(key string, value interface{}) error {
	return c.primary.SetKeyedState(key, value)
}

func (c *compositeStore) GetKeyedState(key string) (interface{}, error) {
	// Try primary first
	v, err := c.primary.GetKeyedState(key)
	if v != nil || err != nil {
		return v, err
	}
	// Try fallback
	return c.fallback.GetKeyedState(key)
}

func (c *compositeStore) GetByPrefix(prefix string) (map[string][]byte, error) {
	// Get from both stores
	m1, err := c.primary.GetByPrefix(prefix)
	if err != nil {
		return nil, err
	}
	m2, err := c.fallback.GetByPrefix(prefix)
	if err != nil {
		return m1, nil
	}
	// Merge maps
	for k, v := range m2 {
		m1[k] = v
	}
	return m1, nil
}
