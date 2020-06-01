package nodes

import (
	"github.com/emqx/kuiper/common"
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
	n := NewSourceNode("test", map[string]string{
		"DATASOURCE": "RFC_READ_TABLE",
		"TYPE":       "test",
	})
	contextLogger := common.Log.WithField("rule", "test")
	ctx := contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
	conf := n.getConf(ctx)
	if !reflect.DeepEqual(result, conf) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", result, conf)
	}
}
