package templates

import (
	"encoding/base64"
	"fmt"
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
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.para, tt.err, err)

		} else if tt.err == "" && !reflect.DeepEqual(tt.expect, string(r)) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.para, tt.expect, string(r))
		}
	}
}

// errstring returns the string representation of an error.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
