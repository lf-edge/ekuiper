package common

import (
	"os"
	"path/filepath"
	"reflect"
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

	if _, f := ks.Get("foo"); f {
		t.Errorf("Should not find the foo key.")
	}

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
