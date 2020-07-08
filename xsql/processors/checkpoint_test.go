package processors

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/test"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Full lifecycle test: Run window rule; trigger checkpoints by mock timer; restart rule; make sure the result is right;
func TestCheckpointCount(t *testing.T) {
	common.IsTesting = true
	var tests = []struct {
		name      string
		sql       string
		size      int
		breakSize int
		cc        int
		r         [][]map[string]interface{}
	}{
		{
			name:      `rule1`,
			sql:       `SELECT * FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			size:      5,
			breakSize: 2,
			cc:        2,
			r: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}, {
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
				}},
				{{
					"color": "red",
					"size":  float64(3),
					"ts":    float64(1541152486013),
				}, {
					"color": "blue",
					"size":  float64(6),
					"ts":    float64(1541152486822),
				}, {
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}},
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}, {
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createStreams(t)
	defer dropStreams(t)
	options := []*api.RuleOption{
		{
			BufferLength:       100,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 1000,
		}, {
			BufferLength:       100,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 1000,
		},
	}
	for j, opt := range options {
		for i, tt := range tests {
			test.ResetClock(1541152486000)
			p := NewRuleProcessor(DbDir)
			parser := xsql.NewParser(strings.NewReader(tt.sql))
			var (
				sources []*nodes.SourceNode
				syncs   []chan int
			)
			if stmt, err := xsql.Language.Parse(parser); err != nil {
				t.Errorf("parse sql %s error: %s", tt.sql, err)
			} else {
				if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
					t.Errorf("sql %s is not a select statement", tt.sql)
				} else {
					streams := xsql.GetStreams(selectStmt)
					for _, stream := range streams {
						next := make(chan int)
						syncs = append(syncs, next)
						source := getMockSource(stream, next, tt.size)
						sources = append(sources, source)
					}
				}
			}
			tp, inputs, err := p.createTopoWithSources(&api.Rule{Id: fmt.Sprintf("%s_%d", tt.name, j), Sql: tt.sql, Options: opt}, sources)
			if err != nil {
				t.Error(err)
			}
			mockSink := test.NewMockSink()
			sink := nodes.NewSinkNodeWithSink("mockSink", mockSink, nil)
			tp.AddSink(inputs, sink)
			errCh := tp.Open()
			func() {
				for i := 0; i < tt.breakSize*len(syncs); i++ {
					syncs[i%len(syncs)] <- i
					for {
						time.Sleep(1)
						if getMetric(tp, "op_window_0_records_in_total") == (i + 1) {
							break
						}
					}
					select {
					case err = <-errCh:
						t.Log(err)
						tp.Cancel()
						return
					default:
					}
				}

				mockClock := test.GetMockClock()
				mockClock.Set(common.TimeFromUnixMilli(int64(1541152486014 + tt.breakSize*1000)))
				actual := tp.GetCoordinator().GetCompleteCount()
				if !reflect.DeepEqual(tt.cc, actual) {
					t.Errorf("%d-%d. checkpoint count\n\nresult mismatch:\n\nexp=%#v\n\ngot=%d\n\n", i, j, tt.cc, actual)
					return
				}
				time.Sleep(1000)
				tp.Cancel()
				//TODO window memory
				//	errCh := tp.Open()
				//	for i := tt.breakSize; i < tt.size*len(syncs); i++ {
				//		syncs[i%len(syncs)] <- i
				//		retry := 100
				//		for ; retry > 0; retry-- {
				//			time.Sleep(1)
				//			if getMetric(tp, "op_window_0_records_in_total") == (i - tt.breakSize + 1) {
				//				break
				//			}
				//		}
				//		select {
				//		case err = <-errCh:
				//			t.Log(err)
				//			tp.Cancel()
				//			return
				//		default:
				//		}
				//	}
				//	time.Sleep(1000)
			}()
			//results := mockSink.GetResults()
			//var maps [][]map[string]interface{}
			//for _, v := range results {
			//	var mapRes []map[string]interface{}
			//	err := json.Unmarshal(v, &mapRes)
			//	if err != nil {
			//		t.Errorf("Failed to parse the input into map")
			//		continue
			//	}
			//	maps = append(maps, mapRes)
			//}
			//if !reflect.DeepEqual(tt.r, maps) {
			//	t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.r, maps)
			//}
			//tp.Cancel()
		}
		cleanStateData()
	}
}
