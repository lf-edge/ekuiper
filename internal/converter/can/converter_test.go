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

package can

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/ngjaying/can"
	"github.com/ngjaying/can/pkg/socketcan"

	"github.com/lf-edge/ekuiper/internal/testx"
)

func TestDecode(t *testing.T) {
	c, err := NewConverter("test")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		m map[string]interface{}
		f can.Frame
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
			f: can.Frame{
				ID:     1414,
				Length: 8,
				Data:   [8]byte{0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x00},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		f := socketcan.Frame{}
		f.EncodeFrame(tt.f)
		data := make([]byte, 16)
		f.MarshalBinary(data)
		a, err := c.Decode(data)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.m, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, tt.m, a)
		}
	}
}
