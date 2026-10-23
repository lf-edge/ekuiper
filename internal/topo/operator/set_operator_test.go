// Copyright 2026 EMQ Technologies Co., Ltd.
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

package operator

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestSetOperator_Apply(t *testing.T) {
	// SELECT device, temperature FROM roomA <op> SELECT device, temperature FROM roomB
	sensorFields := ast.Fields{
		{Name: "device", Expr: &ast.FieldRef{Name: "device"}},
		{Name: "temperature", Expr: &ast.FieldRef{Name: "temperature"}},
	}

	tests := []struct {
		name        string
		opType      ast.SetOpType
		leftFields  ast.Fields
		rightFields ast.Fields
		data        interface{}
		result      interface{}
	}{
		{
			name:        "union removes duplicates within and across sides",
			opType:      ast.UNION_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
			},
		},
		{
			name:        "union all keeps every row",
			opType:      ast.UNION_ALL,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
			},
		},
		{
			name:        "intersect keeps the distinct left rows found on the right",
			opType:      ast.INTERSECT_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-02", "temperature": 22.5}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
		},
		{
			name:        "except keeps the distinct left rows not found on the right",
			opType:      ast.EXCEPT_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": 21.0}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": 21.0}},
				},
			},
		},
		{
			name:        "bigint and float values are compared by value",
			opType:      ast.INTERSECT_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": int64(21)}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 21.0}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": int64(21)}},
				},
			},
		},
		{
			name:        "null values are equal to each other",
			opType:      ast.INTERSECT_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					// a NULL temperature is left out of the projected row
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-04"}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-05", "temperature": 18.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-04"}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-05"}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-04"}},
				},
			},
		},
		{
			name:   "struct values are compared field by field",
			opType: ast.INTERSECT_OP,
			leftFields: ast.Fields{
				{Name: "device", Expr: &ast.FieldRef{Name: "device"}},
				{Name: "location", Expr: &ast.FieldRef{Name: "location"}},
			},
			rightFields: ast.Fields{
				{Name: "device", Expr: &ast.FieldRef{Name: "device"}},
				{Name: "location", Expr: &ast.FieldRef{Name: "location"}},
			},
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "location": map[string]any{"building": "B1", "floor": int64(2)}}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "location": map[string]any{"building": "B1", "floor": int64(3)}}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "location": map[string]any{"floor": 2.0, "building": "B1"}}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-02", "location": map[string]any{"building": "B2", "floor": int64(3)}}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "location": map[string]any{"building": "B1", "floor": int64(2)}}},
				},
			},
		},
		{
			// SELECT device AS id, temperature AS temp FROM roomA INTERSECT SELECT device, temperature FROM roomB
			name:   "columns are matched by position, not by name",
			opType: ast.INTERSECT_OP,
			leftFields: ast.Fields{
				{Name: "device", AName: "id", Expr: &ast.FieldRef{Name: "device"}},
				{Name: "temperature", AName: "temp", Expr: &ast.FieldRef{Name: "temperature"}},
			},
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"id": "thermo-01", "temp": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"id": "thermo-02", "temp": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"id": "thermo-01", "temp": 20.5}},
				},
			},
		},
		{
			// SELECT indoor, outdoor FROM roomA UNION SELECT outdoor, indoor FROM roomB
			name:   "duplicates within a side are found with that side's own columns",
			opType: ast.UNION_OP,
			leftFields: ast.Fields{
				{Name: "indoor", Expr: &ast.FieldRef{Name: "indoor"}},
				{Name: "outdoor", Expr: &ast.FieldRef{Name: "outdoor"}},
			},
			rightFields: ast.Fields{
				{Name: "outdoor", Expr: &ast.FieldRef{Name: "outdoor"}},
				{Name: "indoor", Expr: &ast.FieldRef{Name: "indoor"}},
			},
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"outdoor": 12.0, "indoor": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"outdoor": 21.0, "indoor": 12.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"outdoor": 12.0, "indoor": 21.0}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"outdoor": 12.0, "indoor": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"outdoor": 21.0, "indoor": 12.0}},
				},
			},
		},
		{
			name:   "invisible fields are ignored",
			opType: ast.INTERSECT_OP,
			leftFields: ast.Fields{
				{Name: "device", Expr: &ast.FieldRef{Name: "device"}},
				{Name: "ts", Expr: &ast.FieldRef{Name: "ts"}, Invisible: true},
			},
			rightFields: ast.Fields{
				{Name: "device", Expr: &ast.FieldRef{Name: "device"}},
				{Name: "ts", Expr: &ast.FieldRef{Name: "ts"}, Invisible: true},
			},
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "ts": int64(1760000000000)}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "ts": int64(1760000005000)}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "ts": int64(1760000000000)}},
				},
			},
		},
		{
			name:        "union with an empty left side removes duplicates on the right",
			opType:      ast.UNION_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
			},
		},
		{
			name:        "intersect with an empty right side is empty",
			opType:      ast.INTERSECT_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{},
		},
		{
			name:        "except with an empty right side keeps the distinct left rows",
			opType:      ast.EXCEPT_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
		},
		{
			name:   "large integers keep their exact value",
			opType: ast.INTERSECT_OP,
			leftFields: ast.Fields{
				{Name: "meter", Expr: &ast.FieldRef{Name: "meter"}},
				{Name: "pulses", Expr: &ast.FieldRef{Name: "pulses"}},
			},
			rightFields: ast.Fields{
				{Name: "meter", Expr: &ast.FieldRef{Name: "meter"}},
				{Name: "pulses", Expr: &ast.FieldRef{Name: "pulses"}},
			},
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"meter": "energy-01", "pulses": int64(9007199254740993)}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"meter": "energy-01", "pulses": int64(9007199254740992)}},
				},
			},
			result: &xsql.WindowTuples{},
		},
		{
			name:        "other integer types are compared by value",
			opType:      ast.INTERSECT_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": int32(21)}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": uint8(19)}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-02", "temperature": int64(19)}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": int32(21)}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": uint8(19)}},
				},
			},
		},
		{
			name:        "a float with a fraction does not match an integer",
			opType:      ast.INTERSECT_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 12.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": int64(12)}},
				},
			},
			result: &xsql.WindowTuples{},
		},
		{
			name:   "the same instant in different time zones is equal",
			opType: ast.INTERSECT_OP,
			leftFields: ast.Fields{
				{Name: "device", Expr: &ast.FieldRef{Name: "device"}},
				{Name: "lastSeen", Expr: &ast.FieldRef{Name: "lastSeen"}},
			},
			rightFields: ast.Fields{
				{Name: "device", Expr: &ast.FieldRef{Name: "device"}},
				{Name: "lastSeen", Expr: &ast.FieldRef{Name: "lastSeen"}},
			},
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "lastSeen": time.Date(2026, 10, 13, 8, 30, 0, 0, time.UTC)}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "lastSeen": time.Date(2026, 10, 13, 8, 30, 0, 0, time.UTC)}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "lastSeen": time.Date(2026, 10, 13, 10, 30, 0, 0, time.FixedZone("CEST", 2*3600))}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-02", "lastSeen": time.Date(2026, 10, 13, 8, 30, 0, 0, time.FixedZone("CEST", 2*3600))}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "lastSeen": time.Date(2026, 10, 13, 8, 30, 0, 0, time.UTC)}},
				},
			},
		},
		{
			name:        "intersect all, more copies on the left",
			opType:      ast.INTERSECT_ALL,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
		},
		{
			name:        "except all, more copies on the left",
			opType:      ast.EXCEPT_ALL,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
		},
		{
			name:        "intersect all, more copies on the right",
			opType:      ast.INTERSECT_ALL,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
		},
		{
			name:        "except all, more copies on the right",
			opType:      ast.EXCEPT_ALL,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{},
		},
		{
			name:        "intersect all, mixed rows",
			opType:      ast.INTERSECT_ALL,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
		},
		{
			name:        "except all, mixed rows",
			opType:      ast.EXCEPT_ALL,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "temperature": 21.0}},
				},
			},
		},
		{
			name:        "intersect all, empty right side",
			opType:      ast.INTERSECT_ALL,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{},
		},
		{
			name:        "except all, empty right side",
			opType:      ast.EXCEPT_ALL,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
		},
		{
			// SELECT device, ts INVISIBLE, temperature FROM roomA INTERSECT SELECT device, temperature FROM roomB
			name:   "invisible fields on one side only do not shift the columns",
			opType: ast.INTERSECT_OP,
			leftFields: ast.Fields{
				{Name: "device", Expr: &ast.FieldRef{Name: "device"}},
				{Name: "ts", Expr: &ast.FieldRef{Name: "ts"}, Invisible: true},
				{Name: "temperature", Expr: &ast.FieldRef{Name: "temperature"}},
			},
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "ts": int64(1760000000000), "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-02", "ts": int64(1760000000000), "temperature": 21.0}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-02", "temperature": 23.0}},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "ts": int64(1760000000000), "temperature": 20.5}},
				},
			},
		},
		{
			// SELECT device, temperature INVISIBLE FROM roomA INTERSECT SELECT device, temperature FROM roomB
			name:   "rows with a different number of visible columns are not equal",
			opType: ast.INTERSECT_OP,
			leftFields: ast.Fields{
				{Name: "device", Expr: &ast.FieldRef{Name: "device"}},
				{Name: "temperature", Expr: &ast.FieldRef{Name: "temperature"}, Invisible: true},
			},
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
				},
			},
			result: &xsql.WindowTuples{},
		},
		{
			name:        "the result keeps the window range of the input",
			opType:      ast.UNION_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
				WindowRange: xsql.NewWindowRange(1760000000000, 1760000010000, 1760000010000),
			},
			result: &xsql.WindowTuples{
				Content: []xsql.Row{
					&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
					&xsql.Tuple{Emitter: "roomB", Message: xsql.Message{"device": "thermo-03", "temperature": 19.0}},
				},
				WindowRange: xsql.NewWindowRange(1760000000000, 1760000010000, 1760000010000),
			},
		},
		{
			name:        "an error from upstream is passed on",
			opType:      ast.UNION_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data:        errors.New("an error from upstream"),
			result:      errors.New("an error from upstream"),
		},
		{
			name:        "a single row is rejected",
			opType:      ast.UNION_OP,
			leftFields:  sensorFields,
			rightFields: sensorFields,
			data:        &xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01", "temperature": 20.5}},
			result:      errors.New("run set operator error: set operators must receive a collection"),
		},
	}

	contextLogger := conf.Log.WithField("rule", "TestSetOperator_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := &SetOperator{
				Streams:           []string{"roomA", "roomB"},
				OpType:            tt.opType,
				LeftSelectFields:  tt.leftFields,
				RightSelectFields: tt.rightFields,
			}
			fv, afv := xsql.NewFunctionValuersForOp(nil)
			result := sp.Apply(ctx, tt.data, fv, afv)
			assert.Equal(t, tt.result, result)
		})
	}
}

func TestSetOperator_ApplyWithoutTwoStreams(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "TestSetOperator_ApplyWithoutTwoStreams")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	sp := &SetOperator{
		Streams:           []string{"roomA"},
		OpType:            ast.UNION_OP,
		LeftSelectFields:  ast.Fields{{Name: "device", Expr: &ast.FieldRef{Name: "device"}}},
		RightSelectFields: ast.Fields{{Name: "device", Expr: &ast.FieldRef{Name: "device"}}},
	}
	data := &xsql.WindowTuples{
		Content: []xsql.Row{
			&xsql.Tuple{Emitter: "roomA", Message: xsql.Message{"device": "thermo-01"}},
		},
	}
	fv, afv := xsql.NewFunctionValuersForOp(nil)
	result := sp.Apply(ctx, data, fv, afv)
	assert.Equal(t, errors.New("run set operator error: set operators must receive data from at least two streams"), result)
}
