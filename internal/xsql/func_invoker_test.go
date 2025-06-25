// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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

package xsql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestInvoke(t *testing.T) {
	err := function.Initialize([]binder.FactoryEntry{})
	if err != nil {
		t.Error(err)
		return
	}
	err = validateFuncs("sum", []ast.Expr{})
	assert.Error(t, err)
	assert.EqualError(t, err, "Expect 1 arguments but found 0.")
	err = validateFuncs("window_start", []ast.Expr{})
	if err != nil {
		t.Error(err)
	}
}
