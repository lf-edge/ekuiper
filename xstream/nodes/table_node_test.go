package nodes

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/contexts"
	"github.com/emqx/kuiper/xstream/test"
	"reflect"
	"testing"
)

func TestTableNode(t *testing.T) {
	test.ResetClock(1541152486000)
	var tests = []struct {
		name    string
		options map[string]string
		result  []*xsql.Tuple
	}{
		{ //0
			name: "test0",
			options: map[string]string{
				"TYPE":       "file",
				"DATASOURCE": "lookup.json",
				"CONF_KEY":   "test",
			},
			result: []*xsql.Tuple{
				{
					Emitter: "test0",
					Message: map[string]interface{}{
						"id":   float64(1541152486013),
						"name": "name1",
						"size": float64(2),
					},
					Timestamp: common.GetNowInMilli(),
				},
				{
					Emitter: "test0",
					Message: map[string]interface{}{
						"id":   float64(1541152487632),
						"name": "name2",
						"size": float64(6),
					},
					Timestamp: common.GetNowInMilli(),
				},
				{
					Emitter: "test0",
					Message: map[string]interface{}{
						"id":   float64(1541152489252),
						"name": "name3",
						"size": float64(4),
					},
					Timestamp: common.GetNowInMilli(),
				},
			},
		},
	}

	t.Logf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		n := NewTableNode(tt.name, tt.options)
		resultCh := make(chan interface{})
		errCh := make(chan error)
		contextLogger := common.Log.WithField("test", "test")
		ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
		n.AddOutput(resultCh, "test")
		n.Open(ctx, errCh)
		select {
		case err := <-errCh:
			t.Error(err)
		case d := <-resultCh:
			r, ok := d.([]*xsql.Tuple)
			if !ok {
				t.Errorf("%d. \nresult is not tuple list:got=%#v\n\n", i, d)
				break
			}
			if !reflect.DeepEqual(tt.result, r) {
				t.Errorf("%d. \nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.result, r)
			}
		}
	}
}
