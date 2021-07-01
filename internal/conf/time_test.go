package conf

import (
	"testing"
)

func TestMockClock(t *testing.T) {
	n := GetNowInMilli()
	if n != 0 {
		t.Errorf("mock clock now mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", 0, n)
	}
}
