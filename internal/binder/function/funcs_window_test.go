// Copyright 2023 EMQ Technologies Co., Ltd.
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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestWindowFuncValidate(t *testing.T) {
	testcases := []struct {
		name string
		args []ast.Expr
	}{
		{
			name: "row_number",
			args: nil,
		},
	}

	for _, tc := range testcases {
		f, ok := builtins[tc.name]
		require.True(t, ok)
		err := f.val(nil, tc.args)
		require.NoError(t, err)
	}
}
