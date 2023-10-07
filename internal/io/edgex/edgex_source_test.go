// Copyright 2021-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build edgex || full

package edgex

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	v3 "github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/models"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"

	"github.com/lf-edge/ekuiper/internal/conf"
)

var (
	es      = &EdgexSource{}
	typeMap = map[string]string{
		"b1":  v3.ValueTypeBool,
		"i1":  v3.ValueTypeInt8,
		"i2":  v3.ValueTypeInt16,
		"i3":  v3.ValueTypeInt32,
		"i4":  v3.ValueTypeInt64,
		"i5":  v3.ValueTypeUint8,
		"i6":  v3.ValueTypeUint16,
		"i7":  v3.ValueTypeUint32,
		"s1":  v3.ValueTypeString,
		"f1":  v3.ValueTypeFloat32,
		"f2":  v3.ValueTypeFloat64,
		"i8":  v3.ValueTypeUint64,
		"ba":  v3.ValueTypeBoolArray,
		"ia1": v3.ValueTypeInt8Array,
		"ia2": v3.ValueTypeInt16Array,
		"ia3": v3.ValueTypeInt32Array,
		"ia4": v3.ValueTypeInt64Array,
		"ia5": v3.ValueTypeUint8Array,
		"ia6": v3.ValueTypeUint16Array,
		"ia7": v3.ValueTypeUint32Array,
		"ia8": v3.ValueTypeUint64Array,
		"fa1": v3.ValueTypeFloat32Array,
		"fa2": v3.ValueTypeFloat64Array,
	}
)

func TestGetValue_IntFloat(t *testing.T) {
	testEvent := models.Event{DeviceName: "test"}
	for i := 1; i < 8; i++ {
		name := fmt.Sprintf("i%d", i)
		r1 := models.SimpleReading{
			BaseReading: models.BaseReading{
				ResourceName: name,
				ValueType:    typeMap[name],
			},
			Value: "1",
		}
		testEvent.Readings = append(testEvent.Readings, r1)
	}

	dtoe := dtos.FromEventModelToDTO(testEvent)
	for _, r := range dtoe.Readings {
		if v, e := es.getValue(r, conf.Log); e != nil {
			t.Errorf("%s", e)
		} else {
			expectOne(t, v)
		}
	}

	r1 := dtos.BaseReading{ResourceName: "i8", ValueType: typeMap["i8"], SimpleReading: dtos.SimpleReading{Value: "10796529505058023104"}}
	if v, e := es.getValue(r1, conf.Log); e != nil {
		t.Errorf("%s", e)
	} else {
		if v1, ok := v.(uint64); ok {
			if v1 != 10796529505058023104 {
				t.Errorf("expected 10796529505058023104, but it's %d.", v1)
			}
		}
	}

	r2 := dtos.BaseReading{ResourceName: "f1", ValueType: typeMap["f1"], SimpleReading: dtos.SimpleReading{Value: "3.14"}}
	if v, e := es.getValue(r2, conf.Log); e != nil {
		t.Errorf("%s", e)
	} else {
		if v1, ok := v.(float64); ok {
			if !almostEqual(v1, 3.14) {
				t.Errorf("expected 3.14, but it's %f.", v1)
			}
		}
	}
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= 1e-6
}

func TestGetValue_IntFloatArr(t *testing.T) {
	testEvent := models.Event{DeviceName: "test"}
	for i := 1; i < 8; i++ {
		ia := []int{i, i * 2}
		jsonValue, _ := json.Marshal(ia)
		name := fmt.Sprintf("ia%d", i)
		r1 := models.SimpleReading{
			BaseReading: models.BaseReading{
				ResourceName: name,
				ValueType:    typeMap[name],
			},
			Value: string(jsonValue),
		}
		testEvent.Readings = append(testEvent.Readings, r1)
	}

	dtoe := dtos.FromEventModelToDTO(testEvent)
	for i, r := range dtoe.Readings {
		if v, e := es.getValue(r, conf.Log); e != nil {
			t.Errorf("%s", e)
		} else {
			checkArray(t, i, v)
		}
	}

	r1 := dtos.BaseReading{ResourceName: "ia8", ValueType: typeMap["ia8"], SimpleReading: dtos.SimpleReading{Value: `[10796529505058023104, 10796529505058023105]`}}
	if v, e := es.getValue(r1, conf.Log); e != nil {
		t.Errorf("%s", e)
	} else {
		if v1, ok := v.([]uint64); ok {
			if v1[0] != 10796529505058023104 || v1[1] != 10796529505058023105 {
				t.Errorf("Failed, the array value is not correct %v.", v1)
			}
		} else {
			t.Errorf("expected uint64 array type, but it's %T.", v1)
		}
	}

	rf_00 := dtos.BaseReading{ResourceName: "fa1", ValueType: typeMap["fa1"], SimpleReading: dtos.SimpleReading{Value: `[3.14, 2.71828]`}}
	if v, e := es.getValue(rf_00, conf.Log); e != nil {
		t.Errorf("%s", e)
	} else {
		if v1, ok := v.([]float64); ok {
			if v1[0] != 3.14 || v1[1] != 2.71828 {
				t.Errorf("expected 3.14 & 2.71828, but it's %v.", v1)
			}
		} else {
			t.Errorf("expected float32 array type, but it's %T.", v)
		}
	}
}

func checkArray(t *testing.T, index int, val interface{}) {
	if v1, ok := val.([]int); ok {
		newIdx := index + 1
		if v1[0] != newIdx || v1[1] != newIdx*2 {
			t.Errorf("Failed, the array value is not correct %v.", v1)
		}
	} else {
		t.Errorf("expected int array type, but it's %T.", val)
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
	testEvent := models.Event{DeviceName: "test"}
	for i := 1; i < 3; i++ {
		name := fmt.Sprintf("f%d", i)
		r1 := models.SimpleReading{
			BaseReading: models.BaseReading{
				ResourceName: name,
				ValueType:    typeMap[name],
			},
			Value: "3.14",
		}
		testEvent.Readings = append(testEvent.Readings, r1)
	}

	dtoe := dtos.FromEventModelToDTO(testEvent)
	for _, r := range dtoe.Readings {
		if v, e := es.getValue(r, conf.Log); e != nil {
			t.Errorf("%s", e)
		} else {
			expectPi(t, v)
		}
	}
}

func expectPi(t *testing.T, expected interface{}) {
	if v1, ok := expected.(float64); ok {
		if !almostEqual(v1, 3.14) {
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
		r1 := dtos.BaseReading{ResourceName: "b1", ValueType: typeMap["b1"], SimpleReading: dtos.SimpleReading{Value: v}}
		if v, e := es.getValue(r1, conf.Log); e != nil {
			t.Errorf("%s", e)
		} else {
			expectTrue(t, v)
		}
	}

	r1 := dtos.BaseReading{ResourceName: "b1", ValueType: typeMap["b1"], SimpleReading: dtos.SimpleReading{Value: "TRue"}}
	if _, e := es.getValue(r1, conf.Log); e == nil {
		t.Errorf("%s", e)
	}

	///////////False
	falses := []string{"0", "f", "F", "false", "FALSE", "False"}
	for _, v := range falses {
		r1 := dtos.BaseReading{ResourceName: "b1", ValueType: typeMap["b1"], SimpleReading: dtos.SimpleReading{Value: v}}
		if v, e := es.getValue(r1, conf.Log); e != nil {
			t.Errorf("%s", e)
		} else {
			expectFalse(t, v)
		}
	}

	r1 = dtos.BaseReading{ResourceName: "b1", ValueType: typeMap["b1"], SimpleReading: dtos.SimpleReading{Value: "FAlse"}}
	if _, e := es.getValue(r1, conf.Log); e == nil {
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
	r1 := dtos.BaseReading{ResourceName: "f", ValueType: "FLOAT", SimpleReading: dtos.SimpleReading{Value: "100"}}
	if v, _ := es.getValue(r1, conf.Log); v != "100" {
		t.Errorf("Expected 100, but it's %s!", v)
	}
}

func TestWrongValue(t *testing.T) {
	testEvent := models.Event{DeviceName: "test"}
	// 100 cannot be converted to a boolean value
	r1 := models.SimpleReading{
		BaseReading: models.BaseReading{
			ResourceName: "b1",
			ValueType:    typeMap["b1"],
		},
		Value: "100",
	}
	//'int' string cannot be converted to int value
	r2 := models.SimpleReading{
		BaseReading: models.BaseReading{
			ResourceName: "i1",
			ValueType:    typeMap["i1"],
		},
		Value: "int",
	}
	//'float' string cannot be converted to int value
	r3 := models.SimpleReading{
		BaseReading: models.BaseReading{
			ResourceName: "f1",
			ValueType:    typeMap["f1"],
		},
		Value: "float",
	}
	testEvent.Readings = append(testEvent.Readings, r1, r2, r3)

	dtoe := dtos.FromEventModelToDTO(testEvent)
	for _, v := range dtoe.Readings {
		if _, e := es.getValue(v, conf.Log); e == nil {
			t.Errorf("Expected an error!")
		}
	}
}

func TestPrintConf(t *testing.T) {
	expMbconf := types.MessageBusConfig{Broker: types.HostInfo{Protocol: "tcp", Host: "127.0.0.1", Port: 6625}, Type: "mbus", Optional: map[string]string{
		"proa":     "proa",
		"Password": "fafsadfsadf=",
		"Prob":     "Prob",
	}}
	mbconf := types.MessageBusConfig{Broker: types.HostInfo{Protocol: "tcp", Host: "127.0.0.1", Port: 6625}, Type: "mbus", Optional: map[string]string{
		"proa":     "proa",
		"Password": "fafsadfsadf=",
		"Prob":     "Prob",
	}}
	if !reflect.DeepEqual(expMbconf, mbconf) {
		t.Errorf("conf changed after printing")
	}
}

func TestGetValue_Binary(t *testing.T) {
	ev := []byte("Hello World")
	r1 := dtos.BaseReading{ResourceName: "bin", ValueType: v3.ValueTypeBinary, BinaryReading: dtos.BinaryReading{MediaType: "application/text", BinaryValue: ev}}
	if v, e := es.getValue(r1, conf.Log); e != nil {
		t.Errorf("%s", e)
	} else if !reflect.DeepEqual(ev, v) {
		t.Errorf("result mismatch, expect %v, but got %v", ev, v)
	}
}

func TestGetValue_Object(t *testing.T) {
	ev := map[string]interface{}{
		"a": 3,
		"b": "string",
		"c": 12.3,
	}
	r1 := dtos.NewObjectReading("p", "d", "obj", ev)
	err := r1.Validate()
	if err != nil {
		t.Error(err)
		return
	}
	if v, e := es.getValue(r1, conf.Log); e != nil {
		t.Errorf("%s", e)
	} else if !reflect.DeepEqual(ev, v) {
		t.Errorf("result mismatch, expect %v, but got %v", ev, v)
	}
}
