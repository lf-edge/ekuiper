package common

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSimpleKVStore_Funcs(t *testing.T) {
	abs, _ := filepath.Abs("test.data")
	if f, _ := os.Stat(abs); f != nil {
		_ = os.Remove(abs)
	}

	ks := GetSimpleKVStore(abs)
	if e := ks.Open(); e != nil {
		t.Errorf("Failed to open data %s.", e)
	}

	_ = ks.Set("foo", "bar")
	v, _ := ks.Get("foo")
	reflect.DeepEqual("bar", v)

	_ = ks.Set("foo1", "bar1")
	v1, _ := ks.Get("foo1")
	reflect.DeepEqual("bar1", v1)

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		reflect.DeepEqual(2, len(keys))
	}

	if e2 := ks.Close(); e2 != nil {
		t.Errorf("Failed to close data: %s.", e2)
	}

	//if _, f := ks.Get("foo"); f {
	//	t.Errorf("Should not find the foo key.")
	//}

	_ = ks.Open()
	if v, ok := ks.Get("foo"); ok {
		reflect.DeepEqual("bar", v)
	} else {
		t.Errorf("Should not find the foo key.")
	}

	ks.Delete("foo1")

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		reflect.DeepEqual(1, len(keys))
	}

	_ = os.Remove(abs)
}

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
