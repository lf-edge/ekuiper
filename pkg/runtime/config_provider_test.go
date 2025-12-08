// Copyright 2025 EMQ Technologies Co., Ltd.
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

package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestSetAndGetAppConf(t *testing.T) {
	// Reset state for test isolation
	initialized.Store(false)
	conf = nil

	// Create a test configuration
	testConf := &model.KuiperConf{}
	testConf.Basic.LogLevel = "info"
	testConf.Basic.Debug = true

	// Set the configuration
	SetAppConf(testConf)

	// Verify the configuration is set correctly
	result := GetAppConf()
	require.NotNil(t, result)
	assert.Equal(t, "info", result.Basic.LogLevel)
	assert.True(t, result.Basic.Debug)
}

func TestGetAppConfPanicsBeforeInit(t *testing.T) {
	// Reset state to simulate uninitialized state
	initialized.Store(false)
	conf = nil

	// Verify that GetAppConf panics when called before initialization
	assert.Panics(t, func() {
		GetAppConf()
	}, "GetAppConf should panic when called before SetAppConf")
}

func TestSetAppConfMultipleTimes(t *testing.T) {
	// Reset state for test isolation
	initialized.Store(false)
	conf = nil

	// Set first configuration
	firstConf := &model.KuiperConf{}
	firstConf.Basic.LogLevel = "debug"
	SetAppConf(firstConf)

	// Verify first configuration
	result := GetAppConf()
	assert.Equal(t, "debug", result.Basic.LogLevel)

	// Set second configuration
	secondConf := &model.KuiperConf{}
	secondConf.Basic.LogLevel = "error"
	SetAppConf(secondConf)

	// Verify second configuration overwrites the first
	result = GetAppConf()
	assert.Equal(t, "error", result.Basic.LogLevel)
}
