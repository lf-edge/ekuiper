// Copyright 2025 EMQ Technologies Co., Ltd.
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

package planner

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func findWindowPlan(p LogicalPlan) *WindowPlan {
	if w, ok := p.(*WindowPlan); ok {
		return w
	}
	for _, c := range p.Children() {
		if r := findWindowPlan(c); r != nil {
			return r
		}
	}
	return nil
}

func findFilterPlan(p LogicalPlan) *FilterPlan {
	if fp, ok := p.(*FilterPlan); ok {
		return fp
	}
	for _, c := range p.Children() {
		if r := findFilterPlan(c); r != nil {
			return r
		}
	}
	return nil
}

func setupVehicleStatusStream(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	s, err := json.Marshal(&xsql.StreamInfo{
		StreamType: ast.TypeStream,
		Statement:  `CREATE STREAM vehicle_status (soc BIGINT, charge_status string, ts BIGINT) WITH (DATASOURCE="vehicle_status", FORMAT="json", TIMESTAMP="ts");`,
	})
	require.NoError(t, err)
	require.NoError(t, kv.Set("vehicle_status", string(s)))
}

// TestStateWindowWherePushedToCollect verifies that a WHERE clause on a state
// window becomes the in-window collect filter (collectCondition) of the
// WindowPlan, and not a pre-window filter.
func TestStateWindowWherePushedToCollect(t *testing.T) {
	setupVehicleStatusStream(t)

	sql := `SELECT collect(*) AS charge_data FROM vehicle_status WHERE soc % 10 = 0 AND changed_col(true, soc % 10 = 0) GROUP BY statewindow(charge_status = 'charging', charge_status = 'discharging')`
	stmt, err := xsql.GetStatementFromSql(sql)
	require.NoError(t, err)

	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	p, err := CreateLogicalPlan(stmt, &def.RuleOption{BufferLength: 1024}, kv)
	require.NoError(t, err)

	explain, err := ExplainFromLogicalPlan(p, "charge_monitor")
	require.NoError(t, err)
	require.Contains(t, explain, "collectCondition")

	// Standalone FilterPlan should be gone since condition was pushed into WindowPlan.
	require.Nil(t, findFilterPlan(p), "expected no standalone FilterPlan after pushdown")

	w := findWindowPlan(p)
	require.NotNil(t, w)
	require.NotNil(t, w.collectCondition, "state window should carry the WHERE as collect filter")
	require.Nil(t, w.condition, "state window must not use the WHERE as a pre-window filter")
}

// TestEventWindowWhereKeepsFilterPlan verifies that a WHERE on an event-time
// window is not pushed into the window as a collect filter. Event-time windows
// compute their boundaries (window scheduling, session continuity) from the
// buffered inputs, so rows that do not match the WHERE must still be buffered
// and the filter must run after the window.
func TestEventWindowWhereKeepsFilterPlan(t *testing.T) {
	setupVehicleStatusStream(t)

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "sliding window",
			sql:  `SELECT * FROM vehicle_status WHERE soc % 10 = 0 GROUP BY slidingwindow(ss, 10)`,
		},
		{
			name: "tumbling window",
			sql:  `SELECT * FROM vehicle_status WHERE soc % 10 = 0 GROUP BY tumblingwindow(ss, 10)`,
		},
		{
			name: "session window",
			sql:  `SELECT * FROM vehicle_status WHERE soc % 10 = 0 GROUP BY sessionwindow(ss, 10, 2)`,
		},
		{
			name: "state window",
			sql:  `SELECT * FROM vehicle_status WHERE soc % 10 = 0 GROUP BY statewindow(charge_status = 'charging', charge_status = 'discharging')`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := xsql.GetStatementFromSql(tt.sql)
			require.NoError(t, err)

			o := &def.RuleOption{BufferLength: 1024, IsEventTime: true}
			kv, err := store.GetKV("stream")
			require.NoError(t, err)
			p, err := CreateLogicalPlan(stmt, o, kv)
			require.NoError(t, err)

			require.NotNil(t, findFilterPlan(p), "expected the FilterPlan to be kept above the event-time window")
			w := findWindowPlan(p)
			require.NotNil(t, w)
			require.Nil(t, w.collectCondition, "event-time window must not carry a collect filter")
			require.Nil(t, w.condition, "event-time window must not carry a pre-window filter")
		})
	}
}

// TestWindowWhereAggregateNotCollected verifies that a WHERE depending on an
// aggregate result is never turned into an in-window collect filter, whether
// the aggregate is written directly (rewritten into bypass) or reached through
// a select alias. The aggregate value only exists after the window closes, so
// the FilterPlan must be kept above the window.
func TestWindowWhereAggregateNotCollected(t *testing.T) {
	setupVehicleStatusStream(t)

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "direct aggregate",
			sql:  `SELECT ts FROM vehicle_status WHERE soc > avg(soc) GROUP BY countwindow(5)`,
		},
		{
			name: "aggregate alias",
			sql:  `SELECT ts, avg(soc) AS a FROM vehicle_status WHERE soc > a GROUP BY countwindow(5)`,
		},
		{
			name: "state aggregate alias",
			sql:  `SELECT ts, last_agg_hit_count() AS c FROM vehicle_status WHERE c > 1 GROUP BY countwindow(5)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := xsql.GetStatementFromSql(tt.sql)
			require.NoError(t, err)

			o := &def.RuleOption{BufferLength: 1024}
			kv, err := store.GetKV("stream")
			require.NoError(t, err)
			p, err := CreateLogicalPlan(stmt, o, kv)
			require.NoError(t, err)

			require.NotNil(t, findFilterPlan(p), "expected the FilterPlan to be kept above the window")
			w := findWindowPlan(p)
			require.NotNil(t, w)
			require.Nil(t, w.collectCondition, "aggregate condition must not become a collect filter")
		})
	}
}
