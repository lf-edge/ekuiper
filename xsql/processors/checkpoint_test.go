package processors

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"testing"
	"time"
)

type ruleCheckpointTest struct {
	ruleTest
	pauseSize   int                    // Stop stream after sending pauseSize source to test checkpoint resume
	cc          int                    // checkpoint count when paused
	pauseMetric map[string]interface{} // The metric to check when paused
}

// Full lifecycle test: Run window rule; trigger checkpoints by mock timer; restart rule; make sure the result is right;
func TestCheckpoint(t *testing.T) {
	common.IsTesting = true
	streamList := []string{"demo"}
	handleStream(false, streamList, t)
	var tests = []ruleCheckpointTest{{
		ruleTest: ruleTest{
			name: `TestCheckpointRule1`,
			sql:  `SELECT * FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
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
				{{
					"color": "blue",
					"size":  float64(2),
					"ts":    float64(1541152487632),
				}, {
					"color": "yellow",
					"size":  float64(4),
					"ts":    float64(1541152488442),
				}, {
					"color": "red",
					"size":  float64(1),
					"ts":    float64(1541152489252),
				}},
			},
			m: map[string]interface{}{
				"op_preprocessor_demo_0_records_in_total":  int64(3),
				"op_preprocessor_demo_0_records_out_total": int64(3),

				"op_project_0_records_in_total":  int64(3),
				"op_project_0_records_out_total": int64(3),

				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_demo_0_records_in_total":  int64(3),
				"source_demo_0_records_out_total": int64(3),

				"op_window_0_records_in_total":  int64(3),
				"op_window_0_records_out_total": int64(3),
			},
		},
		pauseSize: 3,
		cc:        2,
		pauseMetric: map[string]interface{}{
			"op_preprocessor_demo_0_records_in_total":  int64(3),
			"op_preprocessor_demo_0_records_out_total": int64(3),

			"op_project_0_records_in_total":  int64(1),
			"op_project_0_records_out_total": int64(1),

			"sink_mockSink_0_records_in_total":  int64(1),
			"sink_mockSink_0_records_out_total": int64(1),

			"source_demo_0_records_in_total":  int64(3),
			"source_demo_0_records_out_total": int64(3),

			"op_window_0_records_in_total":  int64(3),
			"op_window_0_records_out_total": int64(1),
		}},
	}
	handleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength:       100,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 600,
		}, {
			BufferLength:       100,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 600,
		},
	}
	for j, opt := range options {
		doCheckpointRuleTest(t, tests, j, opt)
	}
}

func doCheckpointRuleTest(t *testing.T, tests []ruleCheckpointTest, j int, opt *api.RuleOption) {
	fmt.Printf("The test bucket for option %d size is %d.\n\n", j, len(tests))
	for i, tt := range tests {
		datas, dataLength, tp, mockSink, errCh := createStream(t, tt.ruleTest, j, opt, nil)
		log.Debugf("Start sending first phase data done at %d", common.GetNowInMilli())
		if err := sendData(t, tt.pauseSize, tt.pauseMetric, datas, errCh, tp, 100, 1); err != nil {
			t.Errorf("first phase send data error %s", err)
			break
		}
		log.Debugf("Send first phase data done at %d", common.GetNowInMilli())
		// compare checkpoint count
		time.Sleep(10 * time.Millisecond)
		var retry int
		for retry = 3; retry > 0; retry-- {
			actual := tp.GetCoordinator().GetCompleteCount()
			if tt.cc == actual {
				break
			} else {
				common.Log.Debugf("check checkpointCount error at %d: %d", retry, actual)
			}
			time.Sleep(200 * time.Millisecond)
		}
		cc := tp.GetCoordinator().GetCompleteCount()
		tp.Cancel()
		if retry == 0 {
			t.Errorf("%d-%d. checkpoint count\n\nresult mismatch:\n\nexp=%#v\n\ngot=%d\n\n", i, j, tt.cc, cc)
			return
		} else if retry < 3 {
			fmt.Printf("try %d for checkpoint count\n", 4-retry)
		}
		tp.Cancel()
		time.Sleep(10 * time.Millisecond)
		// resume stream
		log.Debugf("Resume stream at %d", common.GetNowInMilli())
		errCh = tp.Open()
		log.Debugf("After open stream at %d", common.GetNowInMilli())
		if err := sendData(t, dataLength, tt.m, datas, errCh, tp, POSTLEAP, 10); err != nil {
			t.Errorf("second phase send data error %s", err)
			break
		}
		compareResult(t, mockSink, commonResultFunc, tt.ruleTest, i, tp)
	}
}
