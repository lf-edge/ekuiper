package processors

import (
	"reflect"
	"testing"
)

func TestRuleActionParse_Apply(t *testing.T) {
	var tests = []struct {
		ruleStr string
		result  []map[string]interface{}
	}{
		{
			ruleStr: `{
			  "id": "ruleTest",
			  "sql": "SELECT * from demo",
			  "actions": [
				{
				  	"funcName": "RFC_READ_TABLE",
					"ashost":   "192.168.1.100",
					"sysnr":    "02",
					"client":   "900",
					"user":     "SPERF",
					"passwd":   "PASSPASS",
					"params": {
						"QUERY_TABLE": "VBAP",
						"ROWCOUNT":    10,
						"FIELDS": [
							{"FIELDNAME": "MANDT"},
							{"FIELDNAME": "VBELN"},
							{"FIELDNAME": "POSNR"}
						]
					}
				}
			  ]
			}`,
			result: []map[string]interface{}{
				{
					"funcName": "RFC_READ_TABLE",
					"ashost":   "192.168.1.100",
					"sysnr":    "02",
					"client":   "900",
					"user":     "SPERF",
					"passwd":   "PASSPASS",
					"params": map[string]interface{}{
						"QUERY_TABLE": "VBAP",
						"ROWCOUNT":    float64(10),
						"FIELDS": []interface{}{
							map[string]interface{}{"FIELDNAME": "MANDT"},
							map[string]interface{}{"FIELDNAME": "VBELN"},
							map[string]interface{}{"FIELDNAME": "POSNR"},
						},
					},
				},
			},
		},
	}

	p := NewRuleProcessor(DbDir)
	for i, tt := range tests {
		r, err := p.getRuleByJson("ruleTest", tt.ruleStr)
		if err != nil {
			t.Errorf("get rule error: %s", err)
		}
		if !reflect.DeepEqual(tt.result, r.Actions) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, r.Actions)
		}
	}

}
