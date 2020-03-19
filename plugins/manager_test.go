package plugins

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

const endpoint = "http://127.0.0.1/plugins"

func TestManager(t *testing.T) {

	data := []struct {
		t   PluginType
		n   string
		u   string
		err error
	}{
		{
			t:   SOURCE,
			n:   "",
			u:   "",
			err: errors.New("invalid name : should not be empty"),
		}, {
			t: SOURCE,
			n: "random",
			u: endpoint + "/sources/random.zip",
		},
	}
	callback := func() {
		fmt.Printf("callback triggered")
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
		manager, err := NewPluginManager()
		if err != nil {
			t.Error(err)
		}
		err = manager.Register(tt.t, tt.n, tt.u, callback)
		if tt.err != nil {
			if !reflect.DeepEqual(tt.err, err) {
				t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
			}
		} else {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		}
	}
}
