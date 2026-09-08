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

package xsql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestParseLeadUntil(t *testing.T) {
	stmt, err := NewParser(strings.NewReader(
		`SELECT lead(candidate_t2) OVER (WHEN b = true UNTIL ts - current_row(ts) > 5) AS t2 FROM telemetry`,
	)).Parse()
	require.NoError(t, err)

	lead := stmt.Fields[0].Expr.(*ast.Call)
	require.Equal(t, "lead", lead.Name)
	require.NotNil(t, lead.WhenExpr)
	require.NotNil(t, lead.UntilExpr)

	until := lead.UntilExpr.(*ast.BinaryExpr)
	delta := until.LHS.(*ast.BinaryExpr)
	current := delta.RHS.(*ast.Call)
	require.Equal(t, "current_row", current.Name)
}

func TestParseLeadOverEmpty(t *testing.T) {
	_, err := NewParser(strings.NewReader(`SELECT lead(ts) OVER () FROM telemetry`)).Parse()
	require.NoError(t, err)
}

func TestRejectInvalidUntil(t *testing.T) {
	tests := []struct {
		sql string
		err string
	}{
		{`SELECT lead(ts) OVER (UNTIL ts > 5) FROM telemetry`, "UNTIL requires WHEN"},
		{`SELECT lag(ts) OVER (WHEN b UNTIL ts > 5) FROM telemetry`, "UNTIL is only supported for LEAD"},
		{`SELECT current_row(ts) FROM telemetry`, "current_row is only valid inside lead UNTIL"},
		{`SELECT lead() FROM telemetry`, "expect from 1 to 4 args"},
		{`SELECT lead(ts, 0) FROM telemetry`, "positive integer literal"},
		{`SELECT lead(ts, n) FROM telemetry`, "positive integer literal"},
		{`SELECT lead(ts, 1, NULL, 1) FROM telemetry`, "Expect bool"},
		{`SELECT current_row(ts, a) FROM telemetry`, "current_row expects exactly one argument"},
	}
	for _, tt := range tests {
		_, err := NewParser(strings.NewReader(tt.sql)).Parse()
		require.ErrorContains(t, err, tt.err)
	}
}
