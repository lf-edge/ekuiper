package xsql

import (
	"fmt"
	"github.com/emqx/kuiper/common"
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
					"DATASOURCE": "users",
					"FORMAT":     "JSON",
					"KEY":        "USERID",
				},
			},
		},
		{
			s: `CREATE TABLE demo (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "USERID", FieldType: &BasicType{Type: BIGINT}},
				},
				Options: map[string]string{
					"DATASOURCE": "users",
					"FORMAT":     "JSON",
					"KEY":        "USERID",
				},
				StreamType: TypeTable,
			},
		},
		{
			s: `CREATE TABLE table1 (
					name STRING,
					size BIGINT,
					id BIGINT
				) WITH (DATASOURCE="lookup.json", FORMAT="json", CONF_KEY="test");`,
			stmt: &StreamStmt{
				Name: StreamName("table1"),
				StreamFields: []StreamField{
					{Name: "name", FieldType: &BasicType{Type: STRINGS}},
					{Name: "size", FieldType: &BasicType{Type: BIGINT}},
					{Name: "id", FieldType: &BasicType{Type: BIGINT}},
				},
				Options: map[string]string{
					"DATASOURCE": "lookup.json",
					"FORMAT":     "json",
					"CONF_KEY":   "test",
				},
				StreamType: TypeTable,
			},
		},
		{
			s:    `SHOW STREAMS`,
			stmt: &ShowStreamsStatement{},
		},
		{
			s:    `SHOW TABLES`,
			stmt: &ShowTablesStatement{},
		},

		{
			s:    `SHOW STREAMSf`,
			stmt: nil,
			err:  `found "STREAMSf", expected keyword streams or tables.`,
		},

		{
			s:    `SHOW STREAMS d`,
			stmt: nil,
			err:  `found "d", expected semecolon or EOF.`,
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
		{
			s: `DESCRIBE TABLE demo`,
			stmt: &DescribeTableStatement{
				Name: "demo",
			},
			err: ``,
		},

		{
			s: `EXPLAIN TABLE demo1`,
			stmt: &ExplainTableStatement{
				Name: "demo1",
			},
			err: ``,
		},

		{
			s: `DROP TABLE demo1`,
			stmt: &DropTableStatement{
				Name: "demo1",
			},
			err: ``,
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		p := NewParser(strings.NewReader(tt.s))
		stmt, err := Language.Parse(p)
		if !reflect.DeepEqual(tt.err, common.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}

}
