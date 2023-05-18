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

package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFieldResult(t *testing.T) {
	fields := Fields{
		{
			Name:  "Name",
			AName: "AName",
			Expr:  &IndexExpr{},
		},
		{
			Name:  "Name",
			AName: "AName",
			Expr:  &FieldRef{},
		},
		{
			Name:  "Name",
			AName: "",
			Expr:  nil,
		},
		{
			Name:  "",
			AName: "AName",
			Expr:  nil,
		},
	}
	fields.node()
	fields.Swap(0, 1)
	assert.Equal(t, Field{
		Name:  "Name",
		AName: "AName",
		Expr:  &FieldRef{},
	}, fields[0])
	assert.Equal(t, 4, fields.Len())

	for _, f := range fields {
		if f.AName != "" {
			assert.Equal(t, f.AName, f.GetName())
			assert.False(t, f.IsColumn())
			assert.True(t, f.IsSelectionField())
		} else {
			if _, ok := f.Expr.(*FieldRef); ok {
				assert.True(t, f.IsColumn())
				assert.True(t, f.IsSelectionField())
			} else {
				assert.False(t, f.IsColumn())
				assert.False(t, f.IsSelectionField())
			}
			assert.Equal(t, f.Name, f.GetName())
		}
	}
}
