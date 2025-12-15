// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

//go:build full || (rpc && script)

package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/plugin/js"
)

func init() {
	// Wait for other db tests to finish to avoid db lock
	for i := 0; i < 10; i++ {
		if err := js.InitManager(); err != nil {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
}

func TestFullLC(t *testing.T) {
	// Create
	s := new(Server)
	reply := new(string)
	err := s.CreateScript(`nojson`, reply)
	assert.Error(t, err)
	err = s.CreateScript(`{"id": "invalid/test", "script": "function test(a, b) {return a+b}"}`, reply)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalidChar")
	err = s.CreateScript(`{"id": "test", "source": "function() {}"}`, new(string))
	assert.Error(t, err)
	err = s.CreateScript(`{"id": "test", "script": "function test(a, b) {return a+b}"}`, reply)
	assert.NoError(t, err)
	assert.Equal(t, "JavaScript function test is created.", *reply)
	// Get
	err = s.DescScript("test", reply)
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"id\": \"test\",\n  \"description\": \"\",\n  \"script\": \"function test(a, b) {return a+b}\",\n  \"isAgg\": false\n}", *reply)
	// List
	err = s.ShowScripts(0, reply)
	assert.NoError(t, err)
	assert.Equal(t, "[\n  \"test\"\n]", *reply)
	// Delete
	err = s.DropScript("test", reply)
	assert.NoError(t, err)
	assert.Equal(t, "JavaScript function test is dropped", *reply)
	// Get
	err = s.DescScript("test", reply)
	assert.Error(t, err)
	// List
	err = s.ShowScripts(0, reply)
	assert.NoError(t, err)
	assert.Equal(t, "No JavaScript functions are found.", *reply)
	// Delete inexist
	err = s.DropScript("test", reply)
	assert.Error(t, err)
}
