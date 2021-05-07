package nodes

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/contexts"
	"reflect"
	"testing"
)

func TestGetConf_Apply(t *testing.T) {
	result := map[string]interface{}{
		"interval": 1000,
		"ashost":   "192.168.1.100",
		"sysnr":    "02",
		"client":   "900",
		"user":     "SPERF",
		"passwd":   "PASSPASS",
		"format":   "json",
		"params": map[string]interface{}{
			"QUERY_TABLE": "VBAP",
			"ROWCOUNT":    10,
			"FIELDS": []interface{}{
				map[string]interface{}{"FIELDNAME": "MANDT"},
				map[string]interface{}{"FIELDNAME": "VBELN"},
				map[string]interface{}{"FIELDNAME": "POSNR"},
			},
		},
	}
	n := NewSourceNode("test", xsql.TypeStream, &xsql.Options{
		DATASOURCE: "RFC_READ_TABLE",
		TYPE:       "test",
	})
	contextLogger := common.Log.WithField("rule", "test")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	conf := getSourceConf(ctx, n.sourceType, n.options)
	if !reflect.DeepEqual(result, conf) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", result, conf)
	}
}

func TestGetConfAndConvert_Apply(t *testing.T) {
	result := map[string]interface{}{
		"interval": 100,
		"seed":     1,
		"format":   "json",
		"pattern": map[string]interface{}{
			"count": 50,
		},
		"deduplicate": 50,
	}
	n := NewSourceNode("test", xsql.TypeStream, &xsql.Options{
		DATASOURCE: "test",
		TYPE:       "random",
		CONF_KEY:   "dedup",
	})
	contextLogger := common.Log.WithField("rule", "test")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	conf := getSourceConf(ctx, n.sourceType, n.options)
	if !reflect.DeepEqual(result, conf) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", result, conf)
		return
	}

	r := &randomSourceConfig{
		Interval: 100,
		Seed:     1,
		Pattern: map[string]interface{}{
			"count": float64(50),
		},
		Deduplicate: 50,
	}

	cfg := &randomSourceConfig{}
	err := common.MapToStruct(conf, cfg)
	if err != nil {
		t.Errorf("map to sturct error %s", err)
		return
	}

	if !reflect.DeepEqual(r, cfg) {
		t.Errorf("result mismatch:\n\nexp=%v\n\ngot=%v\n\n", r, cfg)
		return
	}
}

type randomSourceConfig struct {
	Interval    int                    `json:"interval"`
	Seed        int                    `json:"seed"`
	Pattern     map[string]interface{} `json:"pattern"`
	Deduplicate int                    `json:"deduplicate"`
}
