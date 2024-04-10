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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
)

func init() {
	testx.InitEnv("js")
	// Wait for other db tests to finish to avoid db lock
	for i := 0; i < 10; i++ {
		if err := InitManager(); err != nil {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
}

func TestCorrectLC(t *testing.T) {
	script := &Script{
		Id:     "testScript",
		Desc:   "Test script",
		Script: "function testScript() { return 'Hello, World!'; }",
		IsAgg:  false,
	}
	err := GetManager().Create(script)
	assert.Nil(t, err)
	result, err := GetManager().GetScript("testScript")
	assert.Nil(t, err)
	assert.Equal(t, script, result)
	keys, err := GetManager().List()
	assert.NoError(t, err)
	assert.Equal(t, []string{"testScript"}, keys)
	err = GetManager().Delete("testScript")
	assert.Nil(t, err)
}

func TestCreateScriptWithInvalidFunction(t *testing.T) {
	script := &Script{
		Id:     "invalidScript",
		Desc:   "Invalid script",
		Script: "function invalidScript() { return 'Hello, World!';",
		IsAgg:  false,
	}
	err := GetManager().Create(script)
	assert.NotNil(t, err)

	err = GetManager().UpsertByJson("invalidScript", "{\"id\":\"invalidScript\",\"description\":\"Invalid script\",\"script\":\"function invalidScript() { return 'Hello, World!';\",\"isAgg\":false}")
	assert.Error(t, err)

	script = &Script{
		Id:     "invalidName",
		Desc:   "Invalid script",
		Script: "function area() { return 'Hello, World!';}",
		IsAgg:  false,
	}
	err = GetManager().Create(script)
	assert.NotNil(t, err)
}

func TestGetScriptNonExistent(t *testing.T) {
	_, err := GetManager().GetScript("nonExistentScript")
	assert.NotNil(t, err)
}

func TestUpdateScript(t *testing.T) {
	script := &Script{
		Id:     "testScript",
		Desc:   "Test script",
		Script: "function testScript(x, y) { return x*y; }",
		IsAgg:  false,
	}
	GetManager().Create(script)
	script.Script = "function testScript() { return x + y; }"
	err := GetManager().Update(script)
	assert.Nil(t, err)
	// Cannot create existing script
	err = GetManager().Create(script)
	assert.NotNil(t, err)
	newScript := &Script{
		Id:     "testScript",
		Desc:   "Test script",
		Script: "function wrongName() { return x + y; }",
	}
	err = GetManager().Update(newScript)
	assert.NotNil(t, err)
	result, err := GetManager().GetScript("testScript")
	assert.Nil(t, err)
	assert.Equal(t, script, result)
	err = GetManager().Delete("testScript")
	assert.Nil(t, err)
}

func TestDeleteNonExistentScript(t *testing.T) {
	err := GetManager().Delete("nonExistentScript")
	assert.NotNil(t, err)
}
