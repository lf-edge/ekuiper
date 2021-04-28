package templates

import (
	"encoding/base64"
	"fmt"
	"github.com/emqx/kuiper/common"
	"reflect"
	"testing"
)

func TestBase64Encode(t *testing.T) {
	var tests = []struct {
		para   interface{}
		expect string
		err    string
	}{
		{
			para:   1,
			expect: "1",
		},

		{
			para:   float32(3.14),
			expect: "3.14",
		},

		{
			para:   float64(3.1415),
			expect: "3.1415",
		},
		{
			para:   "hello",
			expect: "hello",
		},
		{
			para:   "{\"hello\" : 3}",
			expect: "{\"hello\" : 3}",
		},
		{
			para: map[string]interface{}{
				"temperature": 30,
				"humidity":    20,
			},
			expect: `{"humidity":20,"temperature":30}`,
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		result, err := Base64Encode(tt.para)
		r, _ := base64.StdEncoding.DecodeString(result)
		if !reflect.DeepEqual(tt.err, common.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.para, tt.err, err)

		} else if tt.err == "" && !reflect.DeepEqual(tt.expect, string(r)) {
			t.Errorf("%d. %q\n\n mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.para, tt.expect, string(r))
		}
	}
}

func TestAdd(t *testing.T) {
	var tests = []struct {
		para1  interface{}
		para2  interface{}
		expect interface{}
		err    string
	}{
		{
			para1:  int(3),
			para2:  int(3),
			expect: int64(6),
		},
		{
			para1:  int64(3),
			para2:  int64(3),
			expect: int64(6),
		},
		{
			para1:  int64(3),
			para2:  int64(-3),
			expect: int64(0),
		},

		{
			para1:  int(3),
			para2:  float32(3),
			expect: int64(6),
		},
		{
			para1:  int(3),
			para2:  float32(3.14),
			expect: int64(6),
		},
		{
			para1:  int64(3),
			para2:  float64(3.14),
			expect: int64(6),
		},
		{
			para1:  int64(3),
			para2:  uint(1),
			expect: nil,
			err:    "Unsupported data type uint of para2 for Add function when para1 type is int64.",
		},
		{
			para1:  uint64(3),
			para2:  uint64(1),
			expect: uint64(4),
			err:    "",
		},
		{
			para1:  uint64(3),
			para2:  int64(1),
			expect: nil,
			err:    "Unsupported data type int64 of para2 for Add function when para1 type is uint64.",
		},
		{
			para1:  float32(3.1),
			para2:  float32(1.1),
			expect: float64(4.199999928474426),
			err:    "",
		},

		{
			para1:  float64(3.1),
			para2:  float64(1.1),
			expect: float64(4.2),
			err:    "",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		result, err := Add(tt.para1, tt.para2)
		if !reflect.DeepEqual(tt.err, common.Errstring(err)) {
			t.Errorf("%d. %q %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.para1, tt.para2, tt.err, err)

		} else if tt.err == "" && !reflect.DeepEqual(tt.expect, result) {
			t.Errorf("%d. %q %q \n\n mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.para1, tt.para2, tt.expect, result)
		}
	}
}
