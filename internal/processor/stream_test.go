// Copyright 2021-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package processor

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

func init() {
	testx.InitEnv("processor")
}

func TestStreamCreateProcessor(t *testing.T) {
	tests := []struct {
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

	p := NewStreamProcessor()
	for i, tt := range tests {
		results, err := p.ExecStmt(tt.s)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.r, results) {
				t.Errorf("%d. %q\n\nstmt mismatch:\nexp=%s\ngot=%#v\n\n", i, tt.s, tt.r, results)
			}
		}
	}
}

func TestTableProcessor(t *testing.T) {
	tests := []struct {
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
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID", TYPE="mqtt", EXTRA="{\"a\":1}", VERSION="12345");`,
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
				"DATASOURCE: users\nFORMAT: JSON\nKEY: USERID\nTYPE: mqtt\nEXTRA: {\"a\":1}\nVERSION: 12345\n"},
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
	p := NewStreamProcessor()
	for i, tt := range tests {
		results, err := p.ExecStmt(tt.s)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.r, results) {
				t.Errorf("%d. %q\n\nstmt mismatch:\nexp=%s\ngot=%#v\n\n", i, tt.s, tt.r, results)
			}
		}
	}
}

func TestTableList(t *testing.T) {
	p := NewStreamProcessor()
	p.ExecStmt(`CREATE TABLE tt1 () WITH (DATASOURCE="users", FORMAT="JSON", KIND="scan")`)
	p.ExecStmt(`CREATE TABLE tt2 () WITH (DATASOURCE="users", TYPE="memory", FORMAT="JSON", KEY="id", KIND="lookup")`)
	p.ExecStmt(`CREATE TABLE tt3 () WITH (DATASOURCE="users", TYPE="memory", FORMAT="JSON", KEY="id", KIND="lookup")`)
	p.ExecStmt(`CREATE TABLE tt4 () WITH (DATASOURCE="users", FORMAT="JSON")`)
	defer func() {
		p.ExecStmt(`DROP TABLE tt1`)
		p.ExecStmt(`DROP TABLE tt2`)
		p.ExecStmt(`DROP TABLE tt3`)
		p.ExecStmt(`DROP TABLE tt4`)
	}()
	la, err := p.ShowTable("lookup")
	if err != nil {
		t.Errorf("Show lookup table fails: %s", err)
		return
	}
	le := []string{"tt2", "tt3"}
	if !reflect.DeepEqual(le, la) {
		t.Errorf("Show lookup table mismatch:\nexp=%s\ngot=%s", le, la)
		return
	}
	ls, err := p.ShowTable("scan")
	if err != nil {
		t.Errorf("Show scan table fails: %s", err)
		return
	}
	lse := []string{"tt1", "tt4"}
	if !reflect.DeepEqual(lse, ls) {
		t.Errorf("Show scan table mismatch:\nexp=%s\ngot=%s", lse, ls)
		return
	}
}

func TestAll(t *testing.T) {
	expected := map[string]map[string]string{
		"streams": {
			"demo":  "create stream demo () WITH (FORMAT=\"JSON\", DATASOURCE=\"demo\", SHARED=\"TRUE\")",
			"demo1": "create stream demo1 () WITH (FORMAT=\"JSON\", DATASOURCE=\"demo\")",
			"demo2": "create stream demo2 () WITH (FORMAT=\"JSON\", DATASOURCE=\"demo\", SHARED=\"TRUE\")",
			"demo3": "create stream demo3 () WITH (FORMAT=\"JSON\", DATASOURCE=\"demo\", SHARED=\"TRUE\")",
		},
		"tables": {
			"tt1": `CREATE TABLE tt1 () WITH (DATASOURCE="users", FORMAT="JSON", KIND="scan")`,
			"tt3": `CREATE TABLE tt3 () WITH (DATASOURCE="users", TYPE="memory", FORMAT="JSON", KEY="id", KIND="lookup")`,
		},
	}
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()
	for st, m := range expected {
		for k, v := range m {
			p.ExecStmt(v)
			defer p.ExecStmt("Drop " + st + " " + k)
		}
	}
	all, err := p.GetAll()
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(all, expected) {
		t.Errorf("Expect\t %v\nBut got\t%v", expected, all)
	}
}

func TestInferredStream(t *testing.T) {
	// init schema
	// Prepare test schema file
	conf.IsTesting = false
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		t.Fatal(err)
	}
	petcDir := filepath.Join(dataDir, "schemas", "protobuf")
	err = os.MkdirAll(petcDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	// Copy test2.proto
	bytesRead, err := os.ReadFile("../schema/test/test2.proto")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(petcDir, "test2.proto"), bytesRead, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(petcDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	schema.InitRegistry()

	tests := []struct {
		n   string
		s   string
		r   map[string]*ast.JsonStreamField
		err string
	}{
		{
			n: "s1",
			s: `CREATE STREAM demo0 (USERID bigint, NAME string) WITH (FORMAT="JSON", DATASOURCE="demo", SHARED="TRUE")`,
			r: map[string]*ast.JsonStreamField{
				"USERID": {Type: "bigint"},
				"NAME":   {Type: "string"},
			},
		}, {
			n: "s2",
			s: `CREATE STREAM demo1 (USERID bigint, NAME string) WITH (FORMAT="protobuf", DATASOURCE="demo", SCHEMAID="test2.Book")`,
			r: map[string]*ast.JsonStreamField{
				"name":   {Type: "string"},
				"author": {Type: "string"},
			},
		},
	}

	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()
	for i, tt := range tests {
		t.Run(tt.n, func(t *testing.T) {
			_, err := p.ExecStmt(tt.s)
			require.NoError(t, err)
			sf, err := p.GetInferredJsonSchema("demo"+strconv.Itoa(i), ast.TypeStream)
			require.NoError(t, err)
			require.Equal(t, tt.r, sf)
		})
	}
}

func TestDescribeToJson(t *testing.T) {
	tests := []struct {
		s string
		r string
	}{
		{
			s: "Fields\n--------------------------------------------------------------------------------\n" +
				"USERID\tbigint\nFIRST_NAME\tstring\nLAST_NAME\tstring\nNICKNAMES\tarray(string)\nGender\tboolean\nADDRESS\t" +
				"struct(STREET_NAME string, NUMBER bigint)\n\nDATASOURCE: users\nFORMAT: JSON\nKEY: USERID\n",
			r: `{
	"Fields": [
		{
			"Name": "USERID",
			"Type": "bigint"
		},
		{
			"Name": "FIRST_NAME",
			"Type": "string"
		},
		{
			"Name": "LAST_NAME",
			"Type": "string"
		},
		{
			"Name": "NICKNAMES",
			"Type": "array(string)"
		},
		{
			"Name": "Gender",
			"Type": "boolean"
		},
		{
			"Name": "ADDRESS",
			"Type": "struct(STREET_NAME string, NUMBER bigint)"
		}
	],
	"Options": {
		"DATASOURCE:": "users",
		"FORMAT:": "JSON",
		"KEY:": "USERID"
	}
}`,
		},
	}
	for _, tt := range tests {
		s := DescribeToJson(tt.s)
		if !reflect.DeepEqual(tt.r, s) {
			t.Errorf("DescribeToJson mismatch:\nexp=%v\ngot=%v", tt.r, s)
		}
	}
}

func TestErr(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	_, err := p.ExecReplaceStream("1", "2", 1)
	require.Error(t, err)
	_, ok := err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.ExecStreamSql("1")
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.GetStream("1", 1)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.DescStream("1", 1)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.DescStream("1", 1)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.GetInferredJsonSchema("1", 1)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.GetInferredSchema("1", 1)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.execDescribe(&ast.Field{}, 1)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.ExecStreamSql("1")
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.ShowStream(11)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.GetStream("1", 11)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)

	_, err = p.DropStream("1", 11)
	require.Error(t, err)
	_, ok = err.(errorx.ErrorWithCode)
	require.True(t, ok)
}

func TestStreamReplace(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()
	tests := []struct {
		n   string
		s   string
		r   string
		err string
	}{
		{
			n: "init no ver",
			s: `CREATE STREAM r1 () WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			r: "Stream r1 is replaced.",
		},
		{
			n: "update from no to no",
			s: `CREATE STREAM r1 () WITH (DATASOURCE="users", FORMAT="JSON");`,
			r: "Stream r1 is replaced.",
		},
		{
			n: "update from no to yes",
			s: `CREATE STREAM r1 () WITH (DATASOURCE="users", FORMAT="JSON", VERSION="12345");`,
			r: "Stream r1 is replaced.",
		},
		{
			n:   "update from high to low",
			s:   `CREATE STREAM r1 () WITH (DATASOURCE="users", FORMAT="JSON", VERSION="12345");`,
			err: "source r1 already exists with version (12345), new version (12345) is lower",
		},
		{
			n:   "update from same ver",
			s:   `CREATE STREAM r1 () WITH (DATASOURCE="users", FORMAT="JSON", VERSION="12345");`,
			err: "source r1 already exists with version (12345), new version (12345) is lower",
		},
		{
			n:   "update from ver to none",
			s:   `CREATE STREAM r1 () WITH (DATASOURCE="users", FORMAT="JSON");`,
			err: "source r1 already exists with version (12345), new version () is lower",
		},
		{
			n: "update from low to high",
			s: `CREATE STREAM r1 () WITH (DATASOURCE="users", FORMAT="JSON", VERSION="22345");`,
			r: "Stream r1 is replaced.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.n, func(t *testing.T) {
			result, err := p.ExecReplaceStream("r1", tt.s, ast.TypeStream)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.r, result)
			}
		})
	}
}
