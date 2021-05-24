package common

import (
	"reflect"
	"strings"
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

func TestGetDataLoc_Funcs(t *testing.T) {
	d, err := GetDataLoc()
	if err != nil {
		t.Errorf("Errors when getting data loc: %s.", err)
	} else if !strings.HasSuffix(d, "kuiper/data/test") {
		t.Errorf("Unexpected data location %s", d)
	}
}

func TestAbsolutePath(t *testing.T) {
	var tests = []struct {
		r string
		a string
	}{
		{
			r: "etc/services",
			a: "/etc/kuiper/services",
		}, {
			r: "data/",
			a: "/var/lib/kuiper/data/",
		}, {
			r: log_dir,
			a: "/var/log/kuiper",
		}, {
			r: "plugins",
			a: "/var/lib/kuiper/plugins",
		},
	}
	for i, tt := range tests {
		aa, err := absolutePath(tt.r)
		if err != nil {
			t.Errorf("error: %v", err)
		} else {
			if !(tt.a == aa) {
				t.Errorf("%d result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.a, aa)
			}
		}
	}
}
