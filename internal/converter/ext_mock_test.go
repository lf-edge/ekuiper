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

//go:build test

package converter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestEncodeWithMockConverter(t *testing.T) {
	mockConverter := MockConverter{}
	ctx := mockContext.NewMockContext("test", "op1")
	data := map[string]interface{}{
		"temperature": 23.4,
		"humidity":    76,
	}
	encodedData, err := mockConverter.Encode(ctx, data)

	assert.Nil(t, err)
	assert.Contains(t, string(encodedData), `"temperature":23.4`)
	assert.Contains(t, string(encodedData), `"humidity":76`)
}

func TestDecodeWithMockConverter(t *testing.T) {
	mockConverter := MockConverter{}
	ctx := mockContext.NewMockContext("test", "op1")
	data := []byte(`{"temperature":23.4,"humidity":76,"ts":1633027200000}`)
	decodedData, err := mockConverter.Decode(ctx, data)

	assert.Nil(t, err)
	assert.Equal(t, 23.4, decodedData.(map[string]interface{})["temperature"])
	assert.Equal(t, 76, decodedData.(map[string]interface{})["humidity"])
}

func TestGetConverter(t *testing.T) {
	testx.InitEnv("mockconv")
	modules.RegisterSchemaType(modules.PROTOBUF, &schema.PbType{}, ".proto")
	err := schema.InitRegistry()
	require.NoError(t, err)
	ctx := mockContext.NewMockContext("test", "op1")
	_, err = GetOrCreateConverter(ctx, "mock", "a.b", nil, map[string]any{})
	require.NoError(t, err)
	_, err = GetOrCreateConverter(ctx, "mock", "a.b.c.d", nil, map[string]any{})
	require.NoError(t, err)
}
