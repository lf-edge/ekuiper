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

package path

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestAbsPath(t *testing.T) {
	err := os.Setenv("KuiperBaseKey", "/bigdata")
	require.NoError(t, err)
	defer os.Unsetenv("KuiperBaseKey")
	absPath, err := filepath.Abs("/data/uploads/test.txt")
	require.NoError(t, err)
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "absolute path",
			path: absPath,
			want: absPath,
		},
		{
			name: "relative path",
			path: "data/uploads/test.txt",
			want: filepath.Clean("/bigdata/data/uploads/test.txt"),
		},
	}
	ctx := mockContext.NewMockContext("test", "test")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AbsPath(ctx, tt.path)
			assert.Equal(t, tt.want, got)
		})
	}
}
