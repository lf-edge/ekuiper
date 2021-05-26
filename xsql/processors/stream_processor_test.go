package processors

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"path"
	"reflect"
	"testing"
)

var (
	DbDir = common.GetDbDir()
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
			err: "Explain stream fails, topic1 is not found.",
		},
		{
			s: `CREATE STREAM topic1 (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					NICKNAMES ARRAY(STRING),
					Gender BOOLEAN,
					ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT, BUILDING STRUCT(NAME STRING, ROOM BIGINT)),
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
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
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			r: []string{"Stream stream is created."},
		},
		{
			s: `CREATE STREAM topic1 (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			err: "Create stream fails: Item topic1 already exists.",
		},
		{
			s: `EXPLAIN STREAM topic1;`,
			r: []string{"TO BE SUPPORTED"},
		},
		{
			s: `DESCRIBE STREAM topic1;`,
			r: []string{"Fields\n--------------------------------------------------------------------------------\nUSERID\tbigint\nFIRST_NAME\tstring\nLAST_NAME\tstring\nNICKNAMES\t" +
				"array(string)\nGender\tboolean\nADDRESS\tstruct(STREET_NAME string, NUMBER bigint, BUILDING struct(NAME string, ROOM bigint))\n\n" +
				"DATASOURCE: users\nFORMAT: JSON\nKEY: USERID\n"},
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
			err: "Describe stream fails, topic1 is not found.",
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

	streamDB := path.Join(DbDir, "streamTest")
	for i, tt := range tests {
		results, err := NewStreamProcessor(streamDB).ExecStmt(tt.s)
		if !reflect.DeepEqual(tt.err, common.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.r, results) {
				t.Errorf("%d. %q\n\nstmt mismatch:\nexp=%s\ngot=%#v\n\n", i, tt.s, tt.r, results)
			}
		}
	}
}

func TestTableProcessor(t *testing.T) {
	var tests = []struct {
		s   string
		r   []string
		err string
	}{
		{
			s: `SHOW TABLES;`,
			r: []string{"No table definitions are found."},
		},
		{
			s:   `EXPLAIN TABLE topic1;`,
			err: "Explain table fails, topic1 is not found.",
		},
		{
			s: `CREATE TABLE topic1 (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					NICKNAMES ARRAY(STRING),
					Gender BOOLEAN,
					ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			r: []string{"Table topic1 is created."},
		},
		{
			s: `CREATE TABLE ` + "`stream`" + ` (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					NICKNAMES ARRAY(STRING),
					Gender BOOLEAN,
					` + "`地址`" + ` STRUCT(STREET_NAME STRING, NUMBER BIGINT),
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			r: []string{"Table stream is created."},
		},
		{
			s: `CREATE TABLE topic1 (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			err: "Create table fails: Item topic1 already exists.",
		},
		{
			s: `EXPLAIN TABLE topic1;`,
			r: []string{"TO BE SUPPORTED"},
		},
		{
			s: `DESCRIBE TABLE topic1;`,
			r: []string{"Fields\n--------------------------------------------------------------------------------\nUSERID\tbigint\nFIRST_NAME\tstring\nLAST_NAME\tstring\nNICKNAMES\t" +
				"array(string)\nGender\tboolean\nADDRESS\tstruct(STREET_NAME string, NUMBER bigint)\n\n" +
				"DATASOURCE: users\nFORMAT: JSON\nKEY: USERID\n"},
		},
		{
			s: `DROP TABLE topic1;`,
			r: []string{"Table topic1 is dropped."},
		},
		{
			s: `SHOW TABLES;`,
			r: []string{"stream"},
		},
		{
			s:   `DESCRIBE TABLE topic1;`,
			err: "Describe table fails, topic1 is not found.",
		},
		{
			s:   `DROP TABLE topic1;`,
			err: "Drop table fails: topic1 is not found.",
		},
		{
			s: "DROP TABLE `stream`;",
			r: []string{"Table stream is dropped."},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	streamDB := path.Join(common.GetDbDir(), "streamTest")
	for i, tt := range tests {
		results, err := NewStreamProcessor(streamDB).ExecStmt(tt.s)
		if !reflect.DeepEqual(tt.err, common.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.r, results) {
				t.Errorf("%d. %q\n\nstmt mismatch:\nexp=%s\ngot=%#v\n\n", i, tt.s, tt.r, results)
			}
		}
	}
}
