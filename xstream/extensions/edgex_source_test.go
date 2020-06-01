// +build edgex

package extensions

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/models"
	"github.com/emqx/kuiper/common"
	"testing"
)

var es = EdgexSource{valueDescs: map[string]string{
	"b1" : "bool",
	"i1" : "int8",
	"i2" : "INT16",
	"i3" : "INT32",
	"i4" : "INT64",
	"i5" : "UINT8",
	"i6" : "UINT16",
	"i7" : "UINT32",
	"s1" : "String",
	"f1" : "Float32", //FLOAT32 will be handled by special case
	"f2" : "Float64", //FLOAT64 will be handled by special case
	"i8" : "UINT64",  //UINT64 will be handled by special case
	},
}

func TestGetValue_Int(t *testing.T) {
	var testEvent = models.Event{Device: "test"}
	for i := 1; i < 8; i++{
		r1 := models.Reading{Name: fmt.Sprintf("i%d", i), Value: "1"}
		testEvent.Readings = append(testEvent.Readings, r1)
	}

	for _, r := range testEvent.Readings {
		if v, e := es.getValue(r, common.Log); e != nil {
			t.Errorf("%s", e)
		} else {
			expectOne(t, v)
		}
	}

	rf_01 := models.Reading{Name:"f1", Value:"fwtOaw=="}
	if v, e := es.getValue(rf_01, common.Log); e != nil {
		t.Errorf("%s", e)
	} else {
		if v1, ok := v.(float32); ok {
			if v1 != 1.8516986e+38 {
				t.Errorf("expected 1.8516986e+38, but it's %f.", v1)
			}
		}
	}

	r1 := models.Reading{Name: "i8", Value: "10796529505058023104"}
	if v, e := es.getValue(r1, common.Log); e != nil {
		t.Errorf("%s", e)
	} else {
		if v1, ok := v.(uint64); ok {
			if v1 != 10796529505058023104 {
				t.Errorf("expected 10796529505058023104, but it's %d.", v1)
			}
		}
	}
}

func expectOne(t *testing.T, expected interface{}) {
	if v1, ok := expected.(int); ok {
		if v1 != 1 {
			t.Errorf("expected 1, but it's %d.", v1)
		}
	} else {
		t.Errorf("expected int type, but it's %T.", expected)
	}
}

func TestGetValue_Float(t *testing.T) {
	var testEvent = models.Event{Device: "test"}
	for i := 1; i < 3; i++{
		r1 := models.Reading{Name: fmt.Sprintf("f%d", i), Value: "3.14"}
		testEvent.Readings = append(testEvent.Readings, r1)
	}

	for _, r := range testEvent.Readings {
		if v, e := es.getValue(r, common.Log); e != nil {
			t.Errorf("%s", e)
		} else {
			expectPi(t, v)
		}
	}
}

func expectPi(t *testing.T, expected interface{}) {
	if v1, ok := expected.(float64); ok {
		if v1 != 3.14 {
			t.Errorf("expected 3.14, but it's %f.", v1)
		}
	} else {
		t.Errorf("expected float type, but it's %T.", expected)
	}
}


func TestGetValue_Bool(t *testing.T) {
	///////////True
	trues := []string{"1", "t", "T", "true", "TRUE", "True"}
	for _, v := range trues {
		r1 := models.Reading{Name: "b1", Value: v}
		if v, e := es.getValue(r1, common.Log); e != nil {
			t.Errorf("%s", e)
		} else {
			expectTrue(t, v)
		}
	}

	r1 := models.Reading{Name: "b1", Value: "TRue"}
	if _, e := es.getValue(r1, common.Log); e == nil {
		t.Errorf("%s", e)
	}

	///////////False
	falses := []string{"0", "f", "F", "false", "FALSE", "False"}
	for _, v := range falses {
		r1 := models.Reading{Name: "b1", Value: v}
		if v, e := es.getValue(r1, common.Log); e != nil {
			t.Errorf("%s", e)
		} else {
			expectFalse(t, v)
		}
	}

	r1 = models.Reading{Name: "b1", Value: "FAlse"}
	if _, e := es.getValue(r1, common.Log); e == nil {
		t.Errorf("%s", e)
	}
}

func expectTrue(t *testing.T, expected interface{}) {
	if v1, ok := expected.(bool); ok {
		if !v1 {
			t.Errorf("expected true, but it's false.")
		}
	} else {
		t.Errorf("expected boolean type, but it's %t.", expected)
	}
}

func expectFalse(t *testing.T, expected interface{}) {
	if v1, ok := expected.(bool); ok {
		if v1 {
			t.Errorf("expected false, but it's true.")
		}
	} else {
		t.Errorf("expected boolean type, but it's %t.", expected)
	}
}

func TestWrongType(t *testing.T) {
	es1 := EdgexSource{valueDescs: map[string]string{
		"f": "FLOAT", //A not exsited type
		},
	}
	r1 := models.Reading{Name: "f", Value: "100"}
	if v, _ := es1.getValue(r1, common.Log); v != "100" {
		t.Errorf("Expected 100, but it's %s!", v)
	}
}

func TestWrongValue(t *testing.T) {
	var testEvent = models.Event{Device: "test"}
	r1 := models.Reading{Name: "b1", Value: "100"} //100 cannot be converted to a boolean value
	r2 := models.Reading{Name: "i1", Value: "int"} //'int' string cannot be converted to int value
	r3 := models.Reading{Name: "f1", Value: "float"} //'float' string cannot be converted to int value
	testEvent.Readings = append(testEvent.Readings, r1, r2, r3)

	for _, v := range testEvent.Readings {
		if _, e := es.getValue(v, common.Log); e == nil {
			t.Errorf("Expected an error!")
		}
	}
}

func TestCastToString(t *testing.T) {
	if v, ok := CastToString(12); v != "12" || !ok {
		t.Errorf("Failed to cast int.")
	}
	if v, ok := CastToString(true); v != "true" || !ok {
		t.Errorf("Failed to cast bool.")
	}
	if v, ok := CastToString("hello"); v != "hello" || !ok {
		t.Errorf("Failed to cast string.")
	}
	if v, ok := CastToString(12.3); v != "12.30" || !ok {
		t.Errorf("Failed to cast float.")
	}
}