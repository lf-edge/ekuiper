// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
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
	for i, tt := range tests {
		result, err := conv.Decode(ctx, tt.payload)
		if err != nil {
			t.Errorf("%d decode error: %v", i, err)
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d result mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, result)
		}
	}
}

func TestError(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "op1")
	_, err := converter.Encode(ctx, nil)
	require.Error(t, err)
	errWithCode, ok := err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.CovnerterErr, errWithCode.Code())
}
