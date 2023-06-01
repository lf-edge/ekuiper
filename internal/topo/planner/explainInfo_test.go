package planner

import (
	"testing"

	"github.com/lf-edge/ekuiper/pkg/ast"
)

func TestDataSourcePlanExplainInfo(t *testing.T) {
	test := []struct {
		p   *DataSourcePlan
		res string
		t   string
	}{
		{
			p: &DataSourcePlan{
				name: "test1",
				fields: map[string]*ast.JsonStreamField{
					"field1": {},
					"field2": {},
					"field3": {},
				},
			},
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"StreamName: test1, Fields:[ field1, field2, field3 ]\",\"id\":0,\"children\":null}\n",
			t:   "DataSourcePlan",
		},
		{
			p: &DataSourcePlan{
				name: "test2",
				streamFields: map[string]*ast.JsonStreamField{
					"a": {},
					"b": {},
					"c": {},
				},
			},
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"StreamName: test2, StreamFields:[ a, b, c ]\",\"id\":1,\"children\":[0]}\n",
			t:   "DataSourcePlan",
		},
		{
			p: &DataSourcePlan{
				name: "test3",
				fields: map[string]*ast.JsonStreamField{
					"id":      {},
					"column1": {},
					"column2": {},
				},
				streamFields: map[string]*ast.JsonStreamField{
					"s1": {},
					"s2": {},
					"s3": {},
				},
			},
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"StreamName: test3, Fields:[ column1, column2, id ], StreamFields:[ s1, s2, s3 ]\",\"id\":2,\"children\":null}\n",
			t:   "DataSourcePlan",
		},
		{
			p: &DataSourcePlan{
				name: "test4",
			},
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"StreamName: test4\",\"id\":3,\"children\":null}\n",
			t:   "DataSourcePlan",
		},
		{
			p:   &DataSourcePlan{},
			res: "{\"type\":\"DataSourcePlan\",\"info\":\"\",\"id\":4,\"children\":null}\n",
			t:   "DataSourcePlan",
		},
	}

	test[1].p.SetChildren([]LogicalPlan{test[2].p})

	for i := 0; i < len(test); i++ {
		test[i].p = test[i].p.Init()
		test[i].p.BuildExplainInfo(int64(i))
		res := test[i].res
		rty := test[i].t
		explainInfo := test[i].p.Explain()
		ty := test[i].p.Type()
		if explainInfo != res {
			t.Errorf("case %d: expect validate %v but got %v", i, res, explainInfo)
		}
		if ty != rty {
			t.Errorf("case %d: expect validate %v but got %v", i, rty, ty)
		}
	}
}
