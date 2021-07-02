package operator

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/cast"
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
				Content: []xsql.WindowTuples{
					{
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
			},
			result: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
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
		},
		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY id1 DESC",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
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
				WindowRange: &xsql.WindowRange{
					WindowStart: 1541152486013,
					WindowEnd:   1541152487013,
				},
			},
			result: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
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
				WindowRange: &xsql.WindowRange{
					WindowStart: 1541152486013,
					WindowEnd:   1541152487013,
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY id1 DESC",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
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
			},
			result: errors.New("run Order By error: incompatible types for comparison: int and string"),
		},
		{
			sql: "SELECT * FROM src1 WHERE f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY f1, id1 DESC",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
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
			},
			result: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
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
		},
		{
			sql: "SELECT * FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY ts DESC",
			data: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2", "ts": cast.TimeFromUnixMilli(1568854525000)},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854535000)},
							},
						},
					},
				},
			},
			result: xsql.WindowTuplesSet{
				Content: []xsql.WindowTuples{
					{
						Emitter: "src1",
						Tuples: []xsql.Tuple{
							{
								Emitter: "src1",
								Message: xsql.Message{"id1": 3, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854535000)},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 2, "f1": "v2", "ts": cast.TimeFromUnixMilli(1568854525000)},
							}, {
								Emitter: "src1",
								Message: xsql.Message{"id1": 1, "f1": "v1", "ts": cast.TimeFromUnixMilli(1568854515000)},
							},
						},
					},
				},
			},
		},

		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY src1.id1 desc",
			data: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
				},
			},
		},
		{
			sql: "SELECT id1 FROM src1 left join src2 on src1.id1 = src2.id2 WHERE src1.f1 = \"v1\" GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY src2.id2",
			data: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
			result: &xsql.JoinTupleSets{
				Content: []xsql.JoinTuple{
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
							{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
						},
					},
					{
						Tuples: []xsql.Tuple{
							{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
						},
					},
				},
			},
		},

		{
			sql: "SELECT abc FROM tbl group by abc ORDER BY def",
			data: xsql.GroupedTuplesSet{
				{
					Content: []xsql.DataValuer{
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
			result: xsql.GroupedTuplesSet{
				{
					Content: []xsql.DataValuer{
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
		},
		{
			sql: "SELECT id1 FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 ORDER BY id1 desc",
			data: xsql.GroupedTuplesSet{
				{
					Content: []xsql.DataValuer{
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
				{
					Content: []xsql.DataValuer{
						&xsql.Tuple{
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					Content: []xsql.DataValuer{
						&xsql.Tuple{
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2"},
						},
					},
				},
				{
					Content: []xsql.DataValuer{
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
		},
		{
			sql: "SELECT count(*) as c FROM src1 GROUP BY TUMBLINGWINDOW(ss, 10), f1 ORDER BY c",
			data: xsql.GroupedTuplesSet{
				{
					Content: []xsql.DataValuer{
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
				{
					Content: []xsql.DataValuer{
						&xsql.Tuple{
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2", "c": 1},
						},
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					Content: []xsql.DataValuer{
						&xsql.Tuple{
							Emitter: "src1",
							Message: xsql.Message{"id1": 2, "f1": "v2", "c": 1},
						},
					},
				},
				{
					Content: []xsql.DataValuer{
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
		},
		{
			sql: "SELECT src2.id2 FROM src1 left join src2 on src1.id1 = src2.id2 GROUP BY src2.f2, TUMBLINGWINDOW(ss, 10) ORDER BY src2.id2 DESC",
			data: xsql.GroupedTuplesSet{
				{
					Content: []xsql.DataValuer{
						&xsql.JoinTuple{
							Tuples: []xsql.Tuple{
								{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
								{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
							},
						},
					},
					WindowRange: &xsql.WindowRange{
						WindowStart: 1541152486013,
						WindowEnd:   1541152487013,
					},
				},
				{
					Content: []xsql.DataValuer{
						&xsql.JoinTuple{
							Tuples: []xsql.Tuple{
								{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
								{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
							},
						},
					},
					WindowRange: &xsql.WindowRange{
						WindowStart: 1541152486013,
						WindowEnd:   1541152487013,
					},
				},
				{
					Content: []xsql.DataValuer{
						&xsql.JoinTuple{
							Tuples: []xsql.Tuple{
								{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
							},
						},
					},
					WindowRange: &xsql.WindowRange{
						WindowStart: 1541152486013,
						WindowEnd:   1541152487013,
					},
				},
			},
			result: xsql.GroupedTuplesSet{
				{
					Content: []xsql.DataValuer{
						&xsql.JoinTuple{
							Tuples: []xsql.Tuple{
								{Emitter: "src1", Message: xsql.Message{"id1": 2, "f1": "v2"}},
								{Emitter: "src2", Message: xsql.Message{"id2": 4, "f2": "w3"}},
							},
						},
					},
					WindowRange: &xsql.WindowRange{
						WindowStart: 1541152486013,
						WindowEnd:   1541152487013,
					},
				},
				{
					Content: []xsql.DataValuer{
						&xsql.JoinTuple{
							Tuples: []xsql.Tuple{
								{Emitter: "src1", Message: xsql.Message{"id1": 1, "f1": "v1"}},
								{Emitter: "src2", Message: xsql.Message{"id2": 2, "f2": "w2"}},
							},
						},
					},
					WindowRange: &xsql.WindowRange{
						WindowStart: 1541152486013,
						WindowEnd:   1541152487013,
					},
				},
				{
					Content: []xsql.DataValuer{
						&xsql.JoinTuple{
							Tuples: []xsql.Tuple{
								{Emitter: "src1", Message: xsql.Message{"id1": 3, "f1": "v1"}},
							},
						},
					},
					WindowRange: &xsql.WindowRange{
						WindowStart: 1541152486013,
						WindowEnd:   1541152487013,
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestOrderPlan_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("statement parse error %s", err)
			break
		}

		pp := &OrderOp{SortFields: stmt.SortFields}
		fv, afv := xsql.NewFunctionValuersForOp(nil, xsql.FuncRegisters)
		result := pp.Apply(ctx, tt.data, fv, afv)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, result)
		}
	}
}
