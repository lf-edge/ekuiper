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

package js

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

func TestFullLifecycle(t *testing.T) {
	m := GetManager()
	m.Reset()
	// Create scripts
	script := &Script{
		Id:     "testScript",
		Desc:   "Test script",
		Script: "function testScript() { return 'Hello, World!'; }",
		IsAgg:  false,
	}
	err := m.Create(script)
	assert.NoError(t, err)
	script = &Script{
		Id:     "area",
		Desc:   "func for area",
		Script: "function area(x, y) { return x*y; }",
		IsAgg:  false,
	}
	err = m.Create(script)
	assert.NoError(t, err)
	expected := map[string]string{
		"area":       "{\"id\":\"area\",\"description\":\"func for area\",\"script\":\"function area(x, y) { return x*y; }\",\"isAgg\":false}",
		"testScript": "{\"id\":\"testScript\",\"description\":\"Test script\",\"script\":\"function testScript() { return 'Hello, World!'; }\",\"isAgg\":false}",
	}
	// Export scripts
	scripts := m.Export()
	assert.Equal(t, expected, scripts)
	// Partial import with error
	invalidSet := map[string]string{
		"area2":      "{\"id\":\"area2\",\"description\":\"func for area2\",\"script\":\"function area2(x, y) { return x*y; }\",\"isAgg\":false}",
		"testScript": "{\"id\":\"testScript\",\"description\":\"Test script\",\"script\":\"function testScript() { return 'Hello, new!'; }\",\"isAgg\":false}",
		"invalid":    "{\"id\":\"novalid\",\"description\":\"invalid script\",\"script\":\"function novalid() { return 'Hello, invalid!'; }\",\"isAgg\":false}",
	}
	errMap := m.PartialImport(context.Background(), invalidSet)
	assert.Equal(t, map[string]string{"invalid": "the script id novalid does not match the key invalid"}, errMap)
	newExpected := map[string]string{
		"area":       "{\"id\":\"area\",\"description\":\"func for area\",\"script\":\"function area(x, y) { return x*y; }\",\"isAgg\":false}",
		"testScript": "{\"id\":\"testScript\",\"description\":\"Test script\",\"script\":\"function testScript() { return 'Hello, new!'; }\",\"isAgg\":false}",
		"area2":      "{\"id\":\"area2\",\"description\":\"func for area2\",\"script\":\"function area2(x, y) { return x*y; }\",\"isAgg\":false}",
	}
	scripts = m.Export()
	assert.Equal(t, newExpected, scripts)
	// Import scripts
	m.Reset()
	errMap = m.Import(context.Background(), expected)
	assert.Empty(t, errMap)
	status := m.Status()
	assert.Empty(t, status)
	scripts = m.Export()
	assert.Equal(t, expected, scripts)
	// Import with error
	m.Reset()
	scripts = m.Export()
	assert.Empty(t, scripts)
	errMap = m.Import(context.Background(), invalidSet)
	assert.Equal(t, map[string]string{"invalid": "the script id novalid does not match the key invalid"}, errMap)
	status = m.Status()
	assert.Equal(t, map[string]string{"invalid": "the script id novalid does not match the key invalid"}, status)
	newExpected2 := map[string]string{
		"testScript": "{\"id\":\"testScript\",\"description\":\"Test script\",\"script\":\"function testScript() { return 'Hello, new!'; }\",\"isAgg\":false}",
		"area2":      "{\"id\":\"area2\",\"description\":\"func for area2\",\"script\":\"function area2(x, y) { return x*y; }\",\"isAgg\":false}",
	}
	scripts = m.Export()
	assert.Equal(t, newExpected2, scripts)
	// Delete scripts
	m.Reset()
	scripts = m.Export()
	assert.Empty(t, scripts)
}

func TestDBInvalid(t *testing.T) {
	// mock db error
	m := GetManager()
	oldDb := m.db
	m.db = &mockInvalidDB{}
	defer func() {
		m.db = oldDb
	}()
	m.Reset()
	errMap := m.Import(context.Background(), map[string]string{"test": "{\"id\":\"test\",\"description\":\"Test script\",\"script\":\"function test() { return 'Hello, World!'; }\",\"isAgg\":false}"})
	assert.Equal(t, map[string]string{"test": "db is nil"}, errMap)
	errMap = m.PartialImport(context.Background(), map[string]string{"test": "{\"id\":\"test\",\"description\":\"Test script\",\"script\":\"function test() { return 'Hello, World!'; }\",\"isAgg\":false}"})
	assert.Equal(t, map[string]string{"test": "db is nil"}, errMap)
	scripts := m.Export()
	assert.Empty(t, scripts)
	status := m.Status()
	assert.Equal(t, map[string]string{"test": "db is nil"}, status)
	oldStatusDb := m.importStatusDb
	m.importStatusDb = &mockInvalidDB{}
	defer func() {
		m.importStatusDb = oldStatusDb
	}()
	status = m.Status()
	assert.Empty(t, status)
}

type mockInvalidDB struct{}

func (m mockInvalidDB) Setnx(key string, value interface{}) error {
	return fmt.Errorf("db is nil")
}

func (m mockInvalidDB) Set(key string, value interface{}) error {
	return fmt.Errorf("db is nil")
}

func (m mockInvalidDB) Get(key string, val interface{}) (bool, error) {
	return false, fmt.Errorf("db is nil")
}

func (m mockInvalidDB) GetByPrefix(prefix string) (map[string][]byte, error) {
	return nil, fmt.Errorf("db is nil")
}

func (m mockInvalidDB) GetKeyedState(key string) (interface{}, error) {
	return false, fmt.Errorf("db is nil")
}

func (m mockInvalidDB) SetKeyedState(key string, value interface{}) error {
	return fmt.Errorf("db is nil")
}

func (m mockInvalidDB) Delete(key string) error {
	return fmt.Errorf("db is nil")
}

func (m mockInvalidDB) Keys() (keys []string, err error) {
	return nil, fmt.Errorf("db is nil")
}

func (m mockInvalidDB) All() (all map[string]string, err error) {
	return nil, fmt.Errorf("db is nil")
}

func (m mockInvalidDB) Clean() error {
	return fmt.Errorf("db is nil")
}

func (m mockInvalidDB) Drop() error {
	return fmt.Errorf("db is nil")
}
