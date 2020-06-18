package xsql

import (
	"fmt"
	"reflect"
	"testing"
)

func Test_MessageValTest(t *testing.T) {
	var tests = []struct {
		key     string
		message Message
		exptV   interface{}
		exptOk  bool
	}{
		{
			key: "key1",
			message: Message{
				"key1": "val1",
				"key2": "val2",
			},
			exptV:  "val1",
			exptOk: true,
		},

		{
			key: "key0",
			message: Message{
				"key1": "val1",
				"key2": "val2",
			},
			exptV:  nil,
			exptOk: false,
		},

		{
			key: "key1",
			message: Message{
				"Key1": "val1",
				"key2": "val2",
			},
			exptV:  "val1",
			exptOk: true,
		},

		{
			key: "key1" + COLUMN_SEPARATOR + "subkey",
			message: Message{
				"Key1":   "val1",
				"subkey": "subval",
			},
			exptV:  "subval",
			exptOk: true,
		},

		{
			key: "192.168.0.1",
			message: Message{
				"Key1":        "val1",
				"192.168.0.1": "000",
			},
			exptV:  "000",
			exptOk: true,
		},

		{
			key: "parent" + COLUMN_SEPARATOR + "child",
			message: Message{
				"key1":         "val1",
				"child":        "child_val",
				"parent.child": "demo",
			},
			exptV:  "child_val",
			exptOk: true,
		},

		{
			key: "parent.child",
			message: Message{
				"key1":         "val1",
				"child":        "child_val",
				"parent.child": "demo",
			},
			exptV:  "demo",
			exptOk: true,
		},

		{
			key: "parent.Child",
			message: Message{
				"key1":         "val1",
				"child":        "child_val",
				"parent.child": "demo",
			},
			exptV:  "demo",
			exptOk: true,
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		//fmt.Printf("Parsing SQL %q.\n", tt.s)
		v, ok := tt.message.Value(tt.key)
		if tt.exptOk != ok {
			t.Errorf("%d. error mismatch:\n  exp=%t\n  got=%t\n\n", i, tt.exptOk, ok)
		} else if tt.exptOk && !reflect.DeepEqual(tt.exptV, v) {
			t.Errorf("%d. \n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.exptV, v)
		}
	}
}
