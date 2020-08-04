package processors

import (
	"fmt"
	"github.com/emqx/kuiper/xstream"
	"path"
	"reflect"
	"testing"
)

func TestStreamCreateProcessor(t *testing.T) {
	var tests = []struct {
		s   string
		r   []string
		err string
	}{
		{
			s: `SHOW STREAMS;`,
			r: []string{"No stream definitions are found."},
		},
		{
			s:   `EXPLAIN STREAM topic1;`,
			err: "Stream topic1 is not found.",
		},
		{
			s: `CREATE STREAM topic1 (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					NICKNAMES ARRAY(STRING),
					Gender BOOLEAN,
					ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
				) WITH (DATASOURCE="users", FORMAT="AVRO", KEY="USERID");`,
			r: []string{"Stream topic1 is created."},
		},
		{
			s: `CREATE STREAM ` + "`stream`" + ` (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					NICKNAMES ARRAY(STRING),
					Gender BOOLEAN,
					` + "`地址`" + ` STRUCT(STREET_NAME STRING, NUMBER BIGINT),
				) WITH (DATASOURCE="users", FORMAT="AVRO", KEY="USERID");`,
			r: []string{"Stream stream is created."},
		},
		{
			s: `CREATE STREAM topic1 (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="AVRO", KEY="USERID");`,
			err: "Create stream fails: Item topic1 already exists.",
		},
		{
			s: `EXPLAIN STREAM topic1;`,
			r: []string{"TO BE SUPPORTED"},
		},
		{
			s: `DESCRIBE STREAM topic1;`,
			r: []string{"Fields\n--------------------------------------------------------------------------------\nUSERID\tbigint\nFIRST_NAME\tstring\nLAST_NAME\tstring\nNICKNAMES\t" +
				"array(string)\nGender\tboolean\nADDRESS\tstruct(STREET_NAME string, NUMBER bigint)\n\n" +
				"DATASOURCE: users\nFORMAT: AVRO\nKEY: USERID\n"},
		},
		{
			s: `DROP STREAM topic1;`,
			r: []string{"Stream topic1 is dropped."},
		},
		{
			s: `SHOW STREAMS;`,
			r: []string{"stream"},
		},
		{
			s:   `DESCRIBE STREAM topic1;`,
			err: "Stream topic1 is not found.",
		},
		{
			s:   `DROP STREAM topic1;`,
			err: "Drop stream fails: topic1 is not found.",
		},
		{
			s: "DROP STREAM `stream`;",
			r: []string{"Stream stream is dropped."},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	streamDB := path.Join(getDbDir(), "streamTest")
	for i, tt := range tests {
		results, err := NewStreamProcessor(streamDB).ExecStmt(tt.s)
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.r, results) {
				t.Errorf("%d. %q\n\nstmt mismatch:\nexp=%s\ngot=%#v\n\n", i, tt.s, tt.r, results)
			}
		}
	}
}

func getMetric(tp *xstream.TopologyNew, name string) int {
	keys, values := tp.GetMetrics()
	for index, key := range keys {
		if key == name {
			return int(values[index].(int64))
		}
	}
	fmt.Println("can't find " + name)
	return 0
}
