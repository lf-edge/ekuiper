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

package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryKvStore_SetAndGet(t *testing.T) {
	store := NewMemoryKV()

	// Test Set and Get
	err := store.Set("key1", "value1")
	require.NoError(t, err)

	var result string
	ok, err := store.Get("key1", &result)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "value1", result)

	// Test Get non-existent key
	ok, err = store.Get("nonexistent", &result)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestMemoryKvStore_SetInvalidType(t *testing.T) {
	store := NewMemoryKV()

	// Test Set with non-string value
	err := store.Set("key1", 123)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "value must be string")
}

func TestMemoryKvStore_Setnx(t *testing.T) {
	store := NewMemoryKV()

	// Test Setnx on new key
	err := store.Setnx("key1", "value1")
	require.NoError(t, err)

	var result string
	ok, err := store.Get("key1", &result)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "value1", result)

	// Test Setnx on existing key
	err = store.Setnx("key1", "value2")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Verify value hasn't changed
	ok, err = store.Get("key1", &result)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "value1", result)
}

func TestMemoryKvStore_SetnxInvalidType(t *testing.T) {
	store := NewMemoryKV()

	// Test Setnx with non-string value
	err := store.Setnx("key1", 123)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "value must be string")
}

func TestMemoryKvStore_Delete(t *testing.T) {
	store := NewMemoryKV()

	// Set a key
	err := store.Set("key1", "value1")
	require.NoError(t, err)

	// Delete the key
	err = store.Delete("key1")
	require.NoError(t, err)

	// Verify key is deleted
	var result string
	ok, err := store.Get("key1", &result)
	require.NoError(t, err)
	assert.False(t, ok)

	// Delete non-existent key should not error
	err = store.Delete("nonexistent")
	assert.NoError(t, err)
}

func TestMemoryKvStore_Keys(t *testing.T) {
	store := NewMemoryKV()

	// Empty store
	keys, err := store.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)

	// Add some keys
	err = store.Set("key1", "value1")
	require.NoError(t, err)
	err = store.Set("key2", "value2")
	require.NoError(t, err)
	err = store.Set("key3", "value3")
	require.NoError(t, err)

	// Get all keys
	keys, err = store.Keys()
	require.NoError(t, err)
	assert.Len(t, keys, 3)
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
	assert.Contains(t, keys, "key3")
}

func TestMemoryKvStore_All(t *testing.T) {
	store := NewMemoryKV()

	// Empty store
	all, err := store.All()
	require.NoError(t, err)
	assert.Empty(t, all)

	// Add some key-value pairs
	err = store.Set("key1", "value1")
	require.NoError(t, err)
	err = store.Set("key2", "value2")
	require.NoError(t, err)

	// Get all key-value pairs
	all, err = store.All()
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Equal(t, "value1", all["key1"])
	assert.Equal(t, "value2", all["key2"])
}

func TestMemoryKvStore_Clean(t *testing.T) {
	store := NewMemoryKV()

	// Add some keys
	err := store.Set("key1", "value1")
	require.NoError(t, err)
	err = store.Set("key2", "value2")
	require.NoError(t, err)

	// Clean the store
	err = store.Clean()
	require.NoError(t, err)

	// Verify store is empty
	keys, err := store.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestMemoryKvStore_Drop(t *testing.T) {
	store := NewMemoryKV()

	// Add some keys
	err := store.Set("key1", "value1")
	require.NoError(t, err)
	err = store.Set("key2", "value2")
	require.NoError(t, err)

	// Drop the store
	err = store.Drop()
	require.NoError(t, err)

	// Verify store is empty
	keys, err := store.Keys()
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestMemoryKvStore_KeyedState(t *testing.T) {
	store := NewMemoryKV()

	// Test SetKeyedState
	err := store.SetKeyedState("state1", "statevalue1")
	require.NoError(t, err)

	// Test GetKeyedState
	value, err := store.GetKeyedState("state1")
	require.NoError(t, err)
	assert.Equal(t, "statevalue1", value)

	// Test GetKeyedState for non-existent key
	value, err = store.GetKeyedState("nonexistent")
	require.NoError(t, err)
	assert.Nil(t, value)
}

func TestMemoryKvStore_GetByPrefix(t *testing.T) {
	store := NewMemoryKV()

	// Add some keys with different prefixes
	err := store.Set("prefix1_key1", "value1")
	require.NoError(t, err)
	err = store.Set("prefix1_key2", "value2")
	require.NoError(t, err)
	err = store.Set("prefix2_key1", "value3")
	require.NoError(t, err)
	err = store.Set("other_key", "value4")
	require.NoError(t, err)

	// Get by prefix "prefix1_"
	result, err := store.GetByPrefix("prefix1_")
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, []byte("value1"), result["prefix1_key1"])
	assert.Equal(t, []byte("value2"), result["prefix1_key2"])

	// Get by prefix "prefix2_"
	result, err = store.GetByPrefix("prefix2_")
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, []byte("value3"), result["prefix2_key1"])

	// Get by non-matching prefix
	result, err = store.GetByPrefix("nonexistent_")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestMemoryKvStore_Concurrent(t *testing.T) {
	store := NewMemoryKV()

	// Test concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			key := "key" + string(rune('0'+idx))
			value := "value" + string(rune('0'+idx))
			err := store.Set(key, value)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all goroutines to finish
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all keys are present
	keys, err := store.Keys()
	require.NoError(t, err)
	assert.Len(t, keys, 10)
}
