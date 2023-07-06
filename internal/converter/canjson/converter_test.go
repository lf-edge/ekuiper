// Copyright 2023 EMQ Technologies Co., Ltd.
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

package canjson

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/testx"
)

func TestDecode(t *testing.T) {
	c, err := NewConverter("../can/test")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		m map[string]interface{}
		f []byte
		e string
	}{
		{
			m: map[string]interface{}{
				"VBBrkCntlAccel": 0.0,
				"VBTOSLatPstn":   87.125,
				"VBTOSLonPstn":   168.75,
				"VBTOSObjID":     0.0,
				"VBTOSTTC":       46.400000000000006,
			},
			f: []byte(`{"meta":{"id":1},"frames":[{"id":1414, "data":"5465737400000000"}]}`),
		},
		{
			m: map[string]interface{}{
				"ChrgngSttnCapctOfDsttnNav": 464.0,
				"DistToDsttnNav":            0.0,
				"DsttnTypOfNav":             5.0,
				"FICMChrgCtrlReq":           0.0,
				"FICMChrgSttnMchngSta":      0.0,
				"FICMEleccLckCtrlReq":       0.0,
				"FICMOnRutWarmOffReq":       0.0,
				"FICMOnRutWarmOffReqV":      0.0,
				"GudTimeToDsttnNav":         0.0,
				"NavGudcSts":                0.0,
			},
			f: []byte(`{"meta":{"id":1}, "frames":[{"id":1006, "data":"54657374000000005465737400000000"}]}`),
		},
		{
			m: map[string]interface{}{
				"ChrgngSttnCapctOfDsttnNav": 464.0,
				"DistToDsttnNav":            0.0,
				"DsttnTypOfNav":             5.0,
				"FICMChrgCtrlReq":           0.0,
				"FICMChrgSttnMchngSta":      0.0,
				"FICMEleccLckCtrlReq":       0.0,
				"FICMOnRutWarmOffReq":       0.0,
				"FICMOnRutWarmOffReqV":      0.0,
				"GudTimeToDsttnNav":         0.0,
				"NavGudcSts":                0.0,
				"VBBrkCntlAccel":            0.0,
				"VBTOSLatPstn":              87.125,
				"VBTOSLonPstn":              168.75,
				"VBTOSObjID":                0.0,
				"VBTOSTTC":                  46.400000000000006,
			},
			f: []byte(`{"meta":{"id":1}, "frames":[{"id":1006, "data":"54657374000000005465737400000000"},{"id":1414, "data":"5465737400000000"}]}`),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		a, err := c.Decode(tt.f)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.m, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, tt.m, a)
		}
	}
}
