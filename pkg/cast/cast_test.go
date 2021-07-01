package cast

import (
	"reflect"
	"testing"
)

func TestMapConvert_Funcs(t *testing.T) {
	source := map[interface{}]interface{}{
		"QUERY_TABLE": "VBAP",
		"ROWCOUNT":    10,
		"FIELDS": []interface{}{
			map[interface{}]interface{}{"FIELDNAME": "MANDT"},
			map[interface{}]interface{}{"FIELDNAME": "VBELN"},
			map[interface{}]interface{}{"FIELDNAME": "POSNR"},
		},
	}

	exp := map[string]interface{}{
		"QUERY_TABLE": "VBAP",
		"ROWCOUNT":    10,
		"FIELDS": []interface{}{
			map[string]interface{}{"FIELDNAME": "MANDT"},
			map[string]interface{}{"FIELDNAME": "VBELN"},
			map[string]interface{}{"FIELDNAME": "POSNR"},
		},
	}

	got := ConvertMap(source)
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", exp, got)
	}
}
