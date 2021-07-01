package conf

import (
	"strings"
	"testing"
)

func TestAbsolutePath(t *testing.T) {
	var tests = []struct {
		r string
		a string
	}{
		{
			r: "etc/services",
			a: "/etc/kuiper/services",
		}, {
			r: "data/",
			a: "/var/lib/kuiper/data/",
		}, {
			r: logDir,
			a: "/var/log/kuiper",
		}, {
			r: "plugins",
			a: "/var/lib/kuiper/plugins",
		},
	}
	for i, tt := range tests {
		aa, err := absolutePath(tt.r)
		if err != nil {
			t.Errorf("error: %v", err)
		} else {
			if !(tt.a == aa) {
				t.Errorf("%d result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.a, aa)
			}
		}
	}
}

func TestGetDataLoc_Funcs(t *testing.T) {
	d, err := GetDataLoc()
	if err != nil {
		t.Errorf("Errors when getting data loc: %s.", err)
	} else if !strings.HasSuffix(d, "kuiper/data/test") {
		t.Errorf("Unexpected data location %s", d)
	}
}
