package operators

import (
	"errors"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/contexts"
	"reflect"
	"strings"
	"testing"
)

func TestAggregatePlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result xsql.GroupedTuplesSet
	}{
		{
			sql: "SELECT abc FROM tbl group by abc",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
					"def": "hello",
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"abc": int64(6),
							"def": "hello",
						},
					},
				},
			},
		},

		{
			sql: "SELECT abc FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					},
				},
			},
		},
		{
			sql: "SELECT abc FROM src1 GROUP BY id1, TUMBLINGWINDOW(ss, 10), f1",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
		},
		{
			sql: "SELECT abc FROM src1 GROUP BY meta(topic), TUMBLINGWINDOW(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter:  "src1",
							Message:  xsql.Message{"id1": 1, "f1": "v1"},
							Metadata: xsql.Metadata{"topic": "topic1"},
						}, {
							Emitter:  "src1",
							Message:  xsql.Message{"id1": 2, "f1": "v2"},
							Metadata: xsql.Metadata{"topic": "topic2"},
						}, {
							Emitter:  "src1",
							Message:  xsql.Message{"id1": 3, "f1": "v1"},
							Metadata: xsql.Metadata{"topic": "topic1"},
						},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 1, "f1": "v1"},
						Metadata: xsql.Metadata{"topic": "topic1"},
					},
					&xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 3, "f1": "v1"},
						Metadata: xsql.Metadata{"topic": "topic1"},
					},
				},
				{
					&xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 2, "f1": "v2"},
						Metadata: xsql.Metadata{"topic": "topic2"},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY TUMBLINGWINDOW(ss, 10), src1.f1",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY TUMBLINGWINDOW(ss, 10), src1.ts",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854515000)}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2", "ts": common.TimeFromUnixMilli(1568854573431)}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854515000)}},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854515000)}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854515000)}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2", "ts": common.TimeFromUnixMilli(1568854573431)}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT abc FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), CASE WHEN id1 > 1 THEN \"others\" ELSE \"one\" END",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1"},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					},
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestFilterPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		pp := &AggregateOp{Dimensions: stmt.Dimensions.GetGroups()}
		result := pp.Apply(ctx, tt.data, fv, afv)
		gr, ok := result.(xsql.GroupedTuplesSet)
		if !ok {
			t.Errorf("result is not GroupedTuplesSet")
		}
		if len(tt.result) != len(gr) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, gr)
		}

		for _, r := range tt.result {
			matched := false
			for _, gre := range gr {
				if reflect.DeepEqual(r, gre) {
					matched = true
				}
			}
			if !matched {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, r)
			}
		}
	}
}

func TestAggregatePlanGroupAlias_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result xsql.GroupedTuplesSet
	}{
		{
			sql: "SELECT count(*) as c FROM tbl group by abc",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
					"def": "hello",
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"abc": int64(6),
							"def": "hello",
							"c":   1,
						},
					},
				},
			},
		},

		{
			sql: "SELECT count(*) as c FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1", "c": 2},
					},
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1"},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2", "c": 1},
					},
				},
			},
		},
		{
			sql: "SELECT abc, count(*) as c FROM src1 GROUP BY id1, TUMBLINGWINDOW(ss, 10), f1",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 1, "f1": "v1", "c": 1},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2", "c": 1},
					},
				},
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 3, "f1": "v1", "c": 1},
					},
				},
			},
		},
		{
			sql: "SELECT count(*) as c FROM src1 GROUP BY meta(topic), TUMBLINGWINDOW(ss, 10)",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter:  "src1",
							Message:  xsql.Message{"id1": 1, "f1": "v1"},
							Metadata: xsql.Metadata{"topic": "topic1"},
						}, {
							Emitter:  "src1",
							Message:  xsql.Message{"id1": 2, "f1": "v2"},
							Metadata: xsql.Metadata{"topic": "topic2"},
						}, {
							Emitter:  "src1",
							Message:  xsql.Message{"id1": 3, "f1": "v1"},
							Metadata: xsql.Metadata{"topic": "topic1"},
						},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 1, "f1": "v1", "c": 2},
						Metadata: xsql.Metadata{"topic": "topic1"},
					},
					&xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 3, "f1": "v1"},
						Metadata: xsql.Metadata{"topic": "topic1"},
					},
				},
				{
					&xsql.Tuple{
						Emitter:  "src1",
						Message:  xsql.Message{"id1": 2, "f1": "v2", "c": 1},
						Metadata: xsql.Metadata{"topic": "topic2"},
					},
				},
			},
		},
		{
			sql: "SELECT count(*) as c FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1", "c": 1}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2", "c": 1}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
				},
				{
					&xsql.JoinTuple{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1", "c": 1}},
						},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestFilterPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		var aggregateAlias xsql.Fields
		for _, f := range stmt.Fields {
			if f.AName != "" {
				if xsql.HasAggFuncs(f.Expr) {
					aggregateAlias = append(aggregateAlias, f)
				}
			}
		}
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		pp := &AggregateOp{Dimensions: stmt.Dimensions.GetGroups(), Alias: aggregateAlias}
		result := pp.Apply(ctx, tt.data, fv, afv)
		gr, ok := result.(xsql.GroupedTuplesSet)
		if !ok {
			t.Errorf("result is not GroupedTuplesSet")
		}
		if len(tt.result) != len(gr) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, gr)
		}

		for _, r := range tt.result {
			matched := false
			for _, gre := range gr {
				if reflect.DeepEqual(r, gre) {
					matched = true
				}
			}
			if !matched {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, r)
			}
		}
	}
}

func TestAggregatePlanAlias_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: "SELECT count(*) as c FROM demo",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
					"def": "hello",
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
					"def": "hello",
					"c":   1,
				},
			},
		},
		{
			sql: `SELECT count(*) as c FROM src1`,
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 5, "f1": "v1"},
						},
					},
				},
			},
			result: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1", "c": 3},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 5, "f1": "v1"},
						},
					},
				},
			},
		}, {
			sql: "SELECT count(*) as c FROM test Inner Join test1 on test.id = test1.id GROUP BY TumblingWindow(ss, 10)",
			data: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33}},
						{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.55}},
						{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
						{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
					},
				},
			},
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 122.33, "c": 3}},
						{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 1, "a": 68.55}},
						{Emitter: "src2", Message: xsql.Message{"id": 1, "color": "w2"}},
					},
				},
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "test", Message: xsql.Message{"id": 5, "a": 177.51}},
						{Emitter: "src2", Message: xsql.Message{"id": 5, "color": "w2"}},
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestFilterPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		var aggregateAlias xsql.Fields
		for _, f := range stmt.Fields {
			if f.AName != "" {
				if xsql.HasAggFuncs(f.Expr) {
					aggregateAlias = append(aggregateAlias, f)
				}
			}
		}
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		pp := &AggregateOp{Dimensions: stmt.Dimensions.GetGroups(), Alias: aggregateAlias}
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}

func TestAggregatePlanError(t *testing.T) {
	tests := []struct {
		sql    string
		data   interface{}
		result error
	}{
		{
			sql:    "SELECT abc FROM tbl group by abc",
			data:   errors.New("an error from upstream"),
			result: errors.New("an error from upstream"),
		},

		{
			sql: "SELECT abc FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 * 2",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: errors.New("run Group By error: invalid operation string(v1) * int64(2)"),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestFilterPlanError")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}
		fv, afv, _ := xsql.NewFunctionValuersForOp(nil)
		pp := &AggregateOp{Dimensions: stmt.Dimensions.GetGroups()}
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
