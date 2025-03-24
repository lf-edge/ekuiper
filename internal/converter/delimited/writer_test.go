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

package delimited

import (
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestWrite(t *testing.T) {
	tests := []struct {
		name   string
		data   []map[string]any
		result string
	}{
		{
			name: "normal",
			data: []map[string]any{
				{
					"id":    1233,
					"name":  "test",
					"email": "aaa@ee.com",
				},
				{
					"id":    34555,
					"name":  "test",
					"email": nil,
				},
			},
			result: "email,id,name\naaa@ee.com,1233,test\n,34555,test",
		},
		{
			name: "normal2",
			data: []map[string]any{
				{
					"id":    1233,
					"name":  "test",
					"email": "aaa@ee.com",
				},
				{
					"id":    333,
					"name":  "test",
					"email": nil,
				},
			},
			result: "email,id,name\naaa@ee.com,1233,test\n,333,test",
		},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	w, err := NewCsvWriter(ctx, map[string]any{"delimiter": ",", "hasHeader": true})
	require.NoError(t, err)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = w.New(ctx)
			require.NoError(t, err)
			for _, v := range tt.data {
				err = w.Write(ctx, v)
				require.NoError(t, err)
			}
			r, e := w.Flush(ctx)
			require.NoError(t, e)
			require.Equal(t, tt.result, string(r))
		})
	}
}
