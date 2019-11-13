package xsql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestParser_ParseTree(t *testing.T) {
	var tests = []struct {
		s    string
		stmt Statement
		err  string
	}{
		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "USERID", FieldType: &BasicType{Type: BIGINT}},
				},
				Options: map[string]string{
					"DATASOURCE" : "users",
					"FORMAT" : "JSON",
					"KEY" : "USERID",
				},
			},
		},

		{
			s: `SHOW STREAMS`,
			stmt: &ShowStreamsStatement{},
		},

		{
			s: `SHOW STREAMSf`,
			stmt: nil,
			err: `found "STREAMSf", expected keyword streams.`,
		},

		{
			s: `SHOW STREAMS d`,
			stmt: nil,
			err: `found "d", expected semecolon or EOF.`,
		},

		{
			s: `DESCRIBE STREAM demo`,
			stmt: &DescribeStreamStatement{
				Name: "demo",
			},
			err: ``,
		},

		{
			s: `EXPLAIN STREAM demo1`,
			stmt: &ExplainStreamStatement{
				Name: "demo1",
			},
			err: ``,
		},

		{
			s: `DROP STREAM demo1`,
			stmt: &DropStreamStatement{
				Name: "demo1",
			},
			err: ``,
		},

	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		p := NewParser(strings.NewReader(tt.s))
		stmt, err := Language.Parse(p)
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}

}
