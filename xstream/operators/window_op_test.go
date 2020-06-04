package operators

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"reflect"
	"testing"
)

var fivet = []*xsql.Tuple{
	{
		Message: map[string]interface{}{
			"f1": "v1",
		},
	},
	{
		Message: map[string]interface{}{
			"f2": "v2",
		},
	},
	{
		Message: map[string]interface{}{
			"f3": "v3",
		},
	},
	{
		Message: map[string]interface{}{
			"f4": "v4",
		},
	},
	{
		Message: map[string]interface{}{
			"f5": "v5",
		},
	},
}

func TestNewTupleList(t *testing.T) {
	_, e := NewTupleList(nil, 0)
	es1 := "Window size should not be less than zero."
	if !reflect.DeepEqual(es1, e.Error()) {
		t.Errorf("error mismatch:\n  exp=%s\n  got=%s\n\n", es1, e)
	}

	_, e = NewTupleList(nil, 2)
	es1 = "The tuples should not be nil or empty."
	if !reflect.DeepEqual(es1, e.Error()) {
		t.Errorf("error mismatch:\n  exp=%s\n  got=%s\n\n", es1, e)
	}

}

func TestCountWindow(t *testing.T) {
	var tests = []struct {
		tuplelist     TupleList
		expWinCount   int
		winTupleSets  []xsql.WindowTuplesSet
		expRestTuples []*xsql.Tuple
	}{
		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   5,
			},
			expWinCount: 1,
			winTupleSets: []xsql.WindowTuplesSet{
				{
					xsql.WindowTuples{
						Emitter: "",
						Tuples: []xsql.Tuple{
							{
								Message: map[string]interface{}{
									"f1": "v1",
								},
							},
							{
								Message: map[string]interface{}{
									"f2": "v2",
								},
							},
							{
								Message: map[string]interface{}{
									"f3": "v3",
								},
							},
							{
								Message: map[string]interface{}{
									"f4": "v4",
								},
							},
							{
								Message: map[string]interface{}{
									"f5": "v5",
								},
							},
						},
					},
				},
			},
			expRestTuples: []*xsql.Tuple{
				{
					Message: map[string]interface{}{
						"f2": "v2",
					},
				},
				{
					Message: map[string]interface{}{
						"f3": "v3",
					},
				},
				{
					Message: map[string]interface{}{
						"f4": "v4",
					},
				},
				{
					Message: map[string]interface{}{
						"f5": "v5",
					},
				},
			},
		},

		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   3,
			},
			expWinCount: 1,
			winTupleSets: []xsql.WindowTuplesSet{
				{
					xsql.WindowTuples{
						Emitter: "",
						Tuples: []xsql.Tuple{
							{
								Message: map[string]interface{}{
									"f3": "v3",
								},
							},
							{
								Message: map[string]interface{}{
									"f4": "v4",
								},
							},
							{
								Message: map[string]interface{}{
									"f5": "v5",
								},
							},
						},
					},
				},
			},
			expRestTuples: []*xsql.Tuple{
				{
					Message: map[string]interface{}{
						"f4": "v4",
					},
				},
				{
					Message: map[string]interface{}{
						"f5": "v5",
					},
				},
			},
		},

		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   2,
			},
			expWinCount: 1,
			winTupleSets: []xsql.WindowTuplesSet{
				{
					xsql.WindowTuples{
						Emitter: "",
						Tuples: []xsql.Tuple{
							{
								Message: map[string]interface{}{
									"f4": "v4",
								},
							},
							{
								Message: map[string]interface{}{
									"f5": "v5",
								},
							},
						},
					},
				},
			},
			expRestTuples: []*xsql.Tuple{
				{
					Message: map[string]interface{}{
						"f5": "v5",
					},
				},
			},
		},

		{
			tuplelist: TupleList{
				tuples: fivet,
				size:   6,
			},
			expWinCount:  0,
			winTupleSets: nil,
			expRestTuples: []*xsql.Tuple{
				{
					Message: map[string]interface{}{
						"f1": "v1",
					},
				},
				{
					Message: map[string]interface{}{
						"f2": "v2",
					},
				},
				{
					Message: map[string]interface{}{
						"f3": "v3",
					},
				},
				{
					Message: map[string]interface{}{
						"f4": "v4",
					},
				},
				{
					Message: map[string]interface{}{
						"f5": "v5",
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		if tt.expWinCount == 0 {
			if tt.tuplelist.hasMoreCountWindow() {
				t.Errorf("%d \n Should not have more count window.", i)
			}
		} else {
			for j := 0; j < tt.expWinCount; j++ {
				if !tt.tuplelist.hasMoreCountWindow() {
					t.Errorf("%d \n Expect more element, but cannot find more element.", i)
				}
				cw := tt.tuplelist.nextCountWindow()
				if !reflect.DeepEqual(tt.winTupleSets[j], cw) {
					t.Errorf("%d. \nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.winTupleSets[j], cw)
				}
			}

			rest := tt.tuplelist.getRestTuples()
			if !reflect.DeepEqual(tt.expRestTuples, rest) {
				t.Errorf("%d. \nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.expRestTuples, rest)
			}
		}
	}
}
