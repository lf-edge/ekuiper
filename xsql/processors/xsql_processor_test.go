package processors

import (
	"fmt"
	"reflect"
	"testing"
)

func TestStreamCreateProcessor(t *testing.T) {
	const BadgerDir = "D:\\tmp\\test"
	var tests = []struct {
		s    string
		r    []string
		err  string
	}{
		{
			s: `SHOW STREAMS;`,
			r: []string{"no stream definition found"},
		},
		{
			s: `EXPLAIN STREAM demo;`,
			err: "stream demo not found",
		},
		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					NICKNAMES ARRAY(STRING),
					Gender BOOLEAN,
					ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
				) WITH (DATASOURCE="users", FORMAT="AVRO", KEY="USERID");`,
			r: []string{"stream demo created"},
		},
		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="AVRO", KEY="USERID");`,
			err: "key demo already exist, delete it before creating a new one",
		},
		{
			s: `EXPLAIN STREAM demo;`,
			r: []string{"TO BE SUPPORTED"},
		},
		{
			s: `DESCRIBE STREAM demo;`,
			r: []string{"Fields\n--------------------------------------------------------------------------------\nUSERID\tbigint\nFIRST_NAME\tstring\nLAST_NAME\tstring\nNICKNAMES\t" +
				"array(string)\nGender\tboolean\nADDRESS\tstruct(STREET_NAME string, NUMBER bigint)\n\n" +
				"DATASOURCE: users\nFORMAT: AVRO\nKEY: USERID\n"},
		},
		{
			s: `SHOW STREAMS;`,
			r: []string{"demo"},
		},
		{
			s: `DROP STREAM demo;`,
			r: []string{"stream demo dropped"},
		},
		{
			s: `DESCRIBE STREAM demo;`,
			err: "stream demo not found",
		},
		{
			s: `DROP STREAM demo;`,
			err: "Key not found",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		results, err := NewStreamProcessor(tt.s, BadgerDir).Exec()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.r, results) {
				t.Errorf("%d. %q\n\nstmt mismatch:\n\ngot=%#v\n\n", i, tt.s, results)
			}
		}
	}
}
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

