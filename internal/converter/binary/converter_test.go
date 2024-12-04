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

package binary

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestMessageDecode(t *testing.T) {
	image, err := os.ReadFile(path.Join("../../../docs", "cover.jpg"))
	if err != nil {
		t.Errorf("Cannot read image: %v", err)
	}
	tests := []struct {
		payload []byte
		result  map[string]interface{}
	}{
		{
			payload: image,
			result: map[string]interface{}{
				"self": image,
			},
		},
	}
	conv, _ := GetConverter()
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tt := range tests {
		result, err := conv.Decode(ctx, tt.payload)
		require.NoError(t, err)
		require.Equal(t, tt.result, result)
		pp, err := conv.Encode(ctx, result)
		require.NoError(t, err)
		require.Equal(t, tt.payload, pp)
	}
}

func TestError(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "op1")

	m := map[string]string{"test": "test"}
	_, err := converter.Encode(ctx, m)
	assert.EqualError(t, err, "unsupported type map[test:test], must be a map")
	m2 := map[string]any{"test": "test"}
	_, err = converter.Encode(ctx, m2)
	assert.EqualError(t, err, "field self not exist")
	m3 := map[string]any{"test": "test", "self": 23}
	_, err = converter.Encode(ctx, m3)
	assert.EqualError(t, err, "cannot convert int(23) to bytea")
}
