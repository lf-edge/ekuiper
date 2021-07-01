package topotest

import (
	"github.com/emqx/kuiper/internal/conf"
	"github.com/emqx/kuiper/pkg/api"
	"testing"
)

// Full lifecycle test: Run window rule; trigger checkpoints by mock timer; restart rule; make sure the result is right;
func TestCheckpoint(t *testing.T) {
	conf.IsTesting = true
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	var tests = []RuleCheckpointTest{{
		RuleTest: RuleTest{
			Name: `TestCheckpointRule1`,
			Sql:  `SELECT * FROM demo GROUP BY HOPPINGWINDOW(ss, 2, 1)`,
			R: [][]map[string]interface{}{
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
			M: map[string]interface{}{
				"op_1_preprocessor_demo_0_records_in_total":  int64(3),
				"op_1_preprocessor_demo_0_records_out_total": int64(3),

				"op_3_project_0_records_in_total":  int64(3),
				"op_3_project_0_records_out_total": int64(3),

				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),

				"source_demo_0_records_in_total":  int64(3),
				"source_demo_0_records_out_total": int64(3),

				"op_2_window_0_records_in_total":  int64(3),
				"op_2_window_0_records_out_total": int64(3),
			},
		},
		PauseSize: 3,
		Cc:        2,
		PauseMetric: map[string]interface{}{
			"op_1_preprocessor_demo_0_records_in_total":  int64(3),
			"op_1_preprocessor_demo_0_records_out_total": int64(3),

			"op_3_project_0_records_in_total":  int64(1),
			"op_3_project_0_records_out_total": int64(1),

			"sink_mockSink_0_records_in_total":  int64(1),
			"sink_mockSink_0_records_out_total": int64(1),

			"source_demo_0_records_in_total":  int64(3),
			"source_demo_0_records_out_total": int64(3),

			"op_2_window_0_records_in_total":  int64(3),
			"op_2_window_0_records_out_total": int64(1),
		}},
	}
	HandleStream(true, streamList, t)
	options := []*api.RuleOption{
		{
			BufferLength:       100,
			Qos:                api.AtLeastOnce,
			CheckpointInterval: 600,
			SendError:          true,
		}, {
			BufferLength:       100,
			Qos:                api.ExactlyOnce,
			CheckpointInterval: 600,
			SendError:          true,
		},
	}
	for j, opt := range options {
		DoCheckpointRuleTest(t, tests, j, opt)
	}
}
