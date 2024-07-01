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

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/plugin"
)

func TestFactory(t *testing.T) {
	script := &Script{
		Id:     "existingFunction",
		Desc:   "Existing function",
		Script: "function existingFunction() { return 'Hello, World!'; }",
		IsAgg:  false,
	}
	err := GetManager().Create(script)
	assert.NoError(t, err)
	defer func() {
		err := GetManager().Delete("existingFunction")
		assert.NoError(t, err)
	}()
	_, err = GetManager().Function("existingFunction")
	assert.NoError(t, err)
	r := GetManager().HasFunctionSet("existingFunction")
	assert.False(t, r)
	ft, _, _ := GetManager().FunctionPluginInfo("existingFunction")
	assert.Equal(t, plugin.JS_EXTENSION, ft)
	n, ok := GetManager().ConvName("existingFunction")
	assert.True(t, ok)
	assert.Equal(t, "existingFunction", n)
}

func TestNotExistFactory(t *testing.T) {
	_, err := GetManager().Function("existingFunction")
	assert.Error(t, err)
	r := GetManager().HasFunctionSet("existingFunction")
	assert.False(t, r)
	ft, _, _ := GetManager().FunctionPluginInfo("nonExistentFunction")
	assert.Equal(t, plugin.NONE_EXTENSION, ft)
	n, ok := GetManager().ConvName("existingFunction")
	assert.False(t, ok)
	assert.Equal(t, "existingFunction", n)
}
