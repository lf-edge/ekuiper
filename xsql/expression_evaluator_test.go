package xsql

import (
	"fmt"
	"strings"
	"testing"
)

func TestEE(t *testing.T) {
	//stmt, err := NewParser(strings.NewReader(`SELECT * FROM TBL AS t1 WHERE t1.a*2+3>25 AND t1.b='hello'`)).Parse()
	stmt, err := NewParser(strings.NewReader(`SELECT abc FROM tbl WHERE abc*2+3 > 12 AND abc < 20`)).Parse()
	if err != nil {
		t.Errorf("%s.\n", err)
		return
	}

	d := []byte(`{"abc":21, "def":"hello"}`)
	ee := newExpressionEvaluator(d)
	Walk(ee, stmt.Condition)

	if ee.operands.Len() <= 0 {
		t.Error("No operands evaluated")
	} else {
		for {
			if ee.operands.Len() <= 0 {
				break
			}
			fmt.Printf("%s\n", ee.operands.Pop())
		}
	}
}
