package xsql

import (
	"fmt"
	"reflect"
	"testing"
)

func TestLowercaseKeyMap(t *testing.T) {
	var tests = []struct {
		src    map[string]interface{}
		expOrg map[string]interface{}
		dest   map[string]interface{}
	}{
		{
			src: map[string]interface{}{
				"Key1": "value1",
				"key2": "value2",
			},
			expOrg: map[string]interface{} {
				"Key1": nil,
				"key2": nil,
			},
			dest: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
		},

		{
			src: map[string]interface{}{
				"Key1": "value1",
				"Complex": map[string]interface{}{
					"Sub1": "sub_value1",
				},
			},
			expOrg: map[string]interface{} {
				"Key1": nil,
				"Complex": map[string]interface{} {
					"Sub1": nil,
				},
			},
			dest: map[string]interface{}{
				"key1": "value1",
				"complex": map[string]interface{}{
					"sub1": "sub_value1",
				},
			},
		},

		{
			src: map[string]interface{}{
				"Key1": "value1",
				"Complex": map[string]interface{}{
					"Sub1": "sub_value1",
					"Sub1_2": map[string]interface{}{
						"Sub2": "sub2",
					},
				},
			},
			expOrg: map[string]interface{} {
				"Key1": nil,
				"Complex": map[string]interface{}{
					"Sub1": nil,
					"Sub1_2": map[string]interface{}{
						"Sub2": nil,
					},
				},
			},
			dest: map[string]interface{}{
				"key1": "value1",
				"complex": map[string]interface{}{
					"sub1": "sub_value1",
					"sub1_2": map[string]interface{}{
						"sub2": "sub2",
					},
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		//fmt.Printf("Parsing SQL %q.\n", tt.s)
		origKeys := make(map[string]interface{})
		result := LowercaseKeyMap(tt.src, origKeys)
		if !reflect.DeepEqual(tt.dest, result) {
			t.Errorf("%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.dest, result)
		}

		if !reflect.DeepEqual(tt.expOrg, origKeys) {
			t.Errorf("%d. \nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.dest, result)
		}
	}
}
