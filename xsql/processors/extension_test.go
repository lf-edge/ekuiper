// +build !windows

package processors

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/test"
	"os"
	"testing"
	"time"
)

//This cannot be run in Windows. And the plugins must be built to so before running this
//For Windows, run it in wsl with go test xsql/processors/extension_test.go xsql/processors/xsql_processor.go
var CACHE_FILE = "cache"

//Test for source, sink, func and agg func extensions
//The .so files must be in the plugins folder
func TestExtensions(t *testing.T) {
	log := common.Log
	//Reset
	streamList := []string{"ext", "ext2"}
	handleStream(false, streamList, t)
	os.Remove(CACHE_FILE)
	os.Create(CACHE_FILE)
	var tests = []struct {
		name      string
		rj        string
		minLength int
		maxLength int
	}{
		{
			name:      `TestExtensionsRule1`,
			rj:        "{\"sql\": \"SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext where count > 49\",\"actions\": [{\"file\":  {\"path\":\"" + CACHE_FILE + "\"}}]}",
			minLength: 5,
		}, {
			name:      `TestExtensionsRule2`,
			rj:        "{\"sql\": \"SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext2\",\"actions\": [{\"file\":  {\"path\":\"" + CACHE_FILE + "\"}}]}",
			maxLength: 2,
		},
	}
	handleStream(true, streamList, t)
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		// Rest for each test
		cleanStateData()
		test.ResetClock(1541152486000)
		// Create stream
		p := NewRuleProcessor(DbDir)
		p.ExecDrop(tt.name)
		rs, err := p.ExecCreate(tt.name, tt.rj)
		if err != nil {
			t.Errorf("failed to create rule: %s.", err)
			continue
		}
		tp, err := p.ExecInitRule(rs)
		if err != nil {
			t.Errorf("fail to init rule: %v", err)
			continue
		}
		go func() {
			select {
			case err := <-tp.Open():
				log.Println(err)
				tp.Cancel()
			case <-time.After(900 * time.Millisecond):
				tp.Cancel()
			}
		}()
		time.Sleep(1000 * time.Millisecond)
		log.Printf("exit main program after a second")
		results := getResults()
		log.Infof("get results %v", results)
		os.Remove(CACHE_FILE)
		var maps [][]map[string]interface{}
		for _, v := range results {
			var mapRes []map[string]interface{}
			err := json.Unmarshal([]byte(v), &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}

		if tt.minLength > 0 {
			if len(maps) < tt.minLength {
				t.Errorf("%d. %q\n\nresult length is smaller than minlength:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}

		if tt.maxLength > 0 {
			if len(maps) > tt.maxLength {
				t.Errorf("%d. %q\n\nresult length is bigger than maxLength:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}

		for _, r := range maps {
			if len(r) != 1 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			r := r[0]
			c := int((r["c"]).(float64))
			if c != 1 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			e := int((r["e"]).(float64))
			if e != 50 && e != 51 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			p := int(r["p"].(float64))
			if p != 2 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}
	}
}

func getResults() []string {
	f, err := os.Open(CACHE_FILE)
	if err != nil {
		panic(err)
	}
	result := make([]string, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		result = append(result, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	f.Close()
	return result
}

func TestFuncState(t *testing.T) {
	//Reset
	streamList := []string{"text"}
	handleStream(false, streamList, t)
	//Data setup
	var tests = []ruleTest{
		{
			name: `TestFuncStateRule1`,
			sql:  `SELECT accumulateWordCount(slogan, " ") as wc FROM text`,
			r: [][]map[string]interface{}{
				{{
					"wc": float64(3),
				}},
				{{
					"wc": float64(6),
				}},
				{{
					"wc": float64(8),
				}},
				{{
					"wc": float64(16),
				}},
				{{
					"wc": float64(20),
				}},
				{{
					"wc": float64(25),
				}},
				{{
					"wc": float64(27),
				}},
				{{
					"wc": float64(30),
				}},
			},
			m: map[string]interface{}{
				"op_preprocessor_text_0_exceptions_total":   int64(0),
				"op_preprocessor_text_0_process_latency_us": int64(0),
				"op_preprocessor_text_0_records_in_total":   int64(8),
				"op_preprocessor_text_0_records_out_total":  int64(8),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_us": int64(0),
				"op_project_0_records_in_total":   int64(8),
				"op_project_0_records_out_total":  int64(8),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(8),
				"sink_mockSink_0_records_out_total": int64(8),

				"source_text_0_exceptions_total":  int64(0),
				"source_text_0_records_in_total":  int64(8),
				"source_text_0_records_out_total": int64(8),
			},
		},
	}
	handleStream(true, streamList, t)
	doRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
	})
}

func TestFuncStateCheckpoint(t *testing.T) {
	common.IsTesting = true
	streamList := []string{"text"}
	handleStream(false, streamList, t)
	var tests = []ruleCheckpointTest{
		{
			ruleTest: ruleTest{
				name: `TestFuncStateCheckpointRule1`,
				sql:  `SELECT accumulateWordCount(slogan, " ") as wc FROM text`,
				r: [][]map[string]interface{}{
					{{
						"wc": float64(3),
					}},
					{{
						"wc": float64(6),
					}},
					{{
						"wc": float64(8),
					}},
					{{
						"wc": float64(8),
					}},
					{{
						"wc": float64(16),
					}},
					{{
						"wc": float64(20),
					}},
					{{
						"wc": float64(25),
					}},
					{{
						"wc": float64(27),
					}},
					{{
						"wc": float64(30),
					}},
				},
				m: map[string]interface{}{
					"op_preprocessor_text_0_exceptions_total":   int64(0),
					"op_preprocessor_text_0_process_latency_us": int64(0),
					"op_preprocessor_text_0_records_in_total":   int64(6),
					"op_preprocessor_text_0_records_out_total":  int64(6),

					"op_project_0_exceptions_total":   int64(0),
					"op_project_0_process_latency_us": int64(0),
					"op_project_0_records_in_total":   int64(6),
					"op_project_0_records_out_total":  int64(6),

					"sink_mockSink_0_exceptions_total":  int64(0),
					"sink_mockSink_0_records_in_total":  int64(6),
					"sink_mockSink_0_records_out_total": int64(6),

					"source_text_0_exceptions_total":  int64(0),
					"source_text_0_records_in_total":  int64(6),
					"source_text_0_records_out_total": int64(6),
				},
			},
			pauseSize: 3,
			cc:        1,
			pauseMetric: map[string]interface{}{
				"op_preprocessor_text_0_exceptions_total":   int64(0),
				"op_preprocessor_text_0_process_latency_us": int64(0),
				"op_preprocessor_text_0_records_in_total":   int64(3),
				"op_preprocessor_text_0_records_out_total":  int64(3),

				"op_project_0_exceptions_total":   int64(0),
				"op_project_0_process_latency_us": int64(0),
				"op_project_0_records_in_total":   int64(3),
				"op_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_text_0_exceptions_total":  int64(0),
				"source_text_0_records_in_total":  int64(3),
				"source_text_0_records_out_total": int64(3),
			},
		},
	}
	handleStream(true, streamList, t)
	doCheckpointRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength:       100,
		Qos:                api.AtLeastOnce,
		CheckpointInterval: 2000,
	})
}
