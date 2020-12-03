package common

import (
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSimpleKVStore_Funcs(t *testing.T) {
	abs, _ := filepath.Abs("test")
	if f, _ := os.Stat(abs); f != nil {
		os.Remove(abs)
	}

	ks := GetSimpleKVStore(abs)
	if e := ks.Open(); e != nil {
		t.Errorf("Failed to open data %s.", e)
	}

	if err := ks.Setnx("foo", "bar"); nil != err {
		t.Error(err)
	}

	var v string
	if ok := ks.Get("foo", &v); ok {
		if !reflect.DeepEqual("bar", v) {
			t.Error("expect:bar", "get:", v)
		}
	} else {
		t.Errorf("Should not find the foo key.")
	}

	if err := ks.Setnx("foo1", "bar1"); nil != err {
		t.Error(err)
	}

	if err := ks.Set("foo1", "bar2"); nil != err {
		t.Error(err)
	}

	var v1 string
	if ok := ks.Get("foo1", &v1); ok {
		if !reflect.DeepEqual("bar2", v1) {
			t.Error("expect:bar2", "get:", v1)
		}
	} else {
		t.Errorf("Should not find the foo1 key.")
	}

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		if !reflect.DeepEqual(2, len(keys)) {
			t.Error("expect:2", "get:", len(keys))
		}
	}

	if e2 := ks.Close(); e2 != nil {
		t.Errorf("Failed to close data: %s.", e2)
	}

	if err := ks.Open(); nil != err {
		t.Error(err)
	}

	var v2 string
	if ok := ks.Get("foo", &v2); ok {
		if !reflect.DeepEqual("bar", v2) {
			t.Error("expect:bar", "get:", v)
		}
	} else {
		t.Errorf("Should not find the foo key.")
	}

	if err := ks.Delete("foo1"); nil != err {
		t.Error(err)
	}

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		reflect.DeepEqual(1, len(keys))
	}

	if err := ks.Clean(); nil != err {
		t.Error(err)
	}

	if keys, e1 := ks.Keys(); e1 != nil {
		t.Errorf("Failed to get value: %s.", e1)
	} else {
		reflect.DeepEqual(0, len(keys))
	}

	dir, _ := filepath.Split(abs)
	abs = path.Join(dir, "sqliteKV.db")
	os.Remove(abs)

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
