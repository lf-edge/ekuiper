package wasm

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"github.com/lf-edge/ekuiper/internal/testx"
	"reflect"
	"testing"
)

func TestValidate(t *testing.T) {
	var tests = []struct {
		p   *PluginInfo
		err string
	}{
		{
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "mirror",
					Version:    "1.0.0",
					Language:   "go",
					Executable: "mirror.exe",
				},
			},
			err: "invalid plugin, must define at lease one function",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:     "mirror",
					Version:  "1.0.0",
					Language: "go",
				},
				Functions: []string{"aa"},
			},
			err: "invalid plugin, missing executable",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "mirror",
					Version:    "1.0.0",
					Executable: "tt",
				},
				Functions: []string{"aa"},
			},
			err: "invalid plugin, missing language",
		}, {
			p: &PluginInfo{
				PluginMeta: runtime.PluginMeta{
					Name:       "mirror",
					Version:    "1.0.0",
					Language:   "c",
					Executable: "tt",
				},
				Functions: []string{"aa"},
			},
			err: "invalid plugin, language 'c' is not supported",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		err := tt.p.Validate("mirror")
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d error mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.err, err.Error())
		}
		fmt.Println("err: ", err)
	}
}
