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

func TestOrderPlan_Apply(t *testing.T) {
	var tests = []struct {
		sql    string
		data   interface{}
		result interface{}
	}{
		{
			sql: "SELECT * FROM tbl WHERE abc*2+3 > 12 AND abc < 20 ORDER BY abc",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(6),
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl WHERE abc*2+3 > 12 OR def = \"hello\"",
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(34),
					"def": "hello",
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"abc": int64(34),
					"def": "hello",
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY id1 DESC",
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
			result: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY id1 DESC",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
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
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"f1": "v2"},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY id1 DESC",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": "2string", "f1": "v2"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						},
					},
				},
			},
			result: errors.New("run Order By error: incompatible types for comparison: int and string"),
		},
		{
			sql: "SELECT * FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY f1, id1 DESC",
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
			result: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1"},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						},
					},
				},
			},
		},
		{
			sql: "SELECT * FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY ts DESC",
			data: xsql.WindowTuplesSet{
				xsql.WindowTuples{
					Emitter: "src1",
					Tuples: []xsql.Tuple{
						{
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854515000)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2", "ts": common.TimeFromUnixMilli(1568854525000)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 3, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854535000)},
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
							Message: xsql.Message{"id1": 3, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854535000)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2", "ts": common.TimeFromUnixMilli(1568854525000)},
						}, {
							Emitter: "src1",
							Message: xsql.Message{"id1": 1, "f1": "v1", "ts": common.TimeFromUnixMilli(1568854515000)},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY src1.id1 desc",
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
			result: xsql.JoinTupleSets{
				xsql.JoinTuple{
					Tuples: []xsql.Tuple{
						{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
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
						{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
						{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY src2.id2",
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
			result: xsql.JoinTupleSets{
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
		},

		{
			sql: "SELECT abc FROM tbl group by abc ORDER BY def",
			data: xsql.GroupedTuplesSet{
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
			sql: "SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 ORDER BY id1 desc",
			data: xsql.GroupedTuplesSet{
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
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2"},
					},
				},
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
			},
		},
		{
			sql: "SELECT count(*) as c FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 ORDER BY c",
			data: xsql.GroupedTuplesSet{
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
			result: xsql.GroupedTuplesSet{
				{
					&xsql.Tuple{
						Emitter: "src1",
						Message: xsql.Message{"id1": 2, "f1": "v2", "c": 1},
					},
				},
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
			},
		},
		{
			sql: "SELECT src2.id2 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10) ORDER BY src2.id2 DESC",
			data: xsql.GroupedTuplesSet{
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
			result: xsql.GroupedTuplesSet{
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
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
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
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := common.Log.WithField("rule", "TestOrderPlan_Apply")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		pp := &OrderOp{SortFields: stmt.SortFields}
		fv, afv := xsql.NewFunctionValuersForOp(nil)
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
