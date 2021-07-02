package planner

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"path"
	"reflect"
	"strings"
	"testing"
)

type errorStruct struct {
	err  string
	serr *string
}

func newErrorStruct(err string) *errorStruct {
	return &errorStruct{
		err: err,
	}
}

func newErrorStructWithS(err string, serr string) *errorStruct {
	return &errorStruct{
		err:  err,
		serr: &serr,
	}
}

func (e *errorStruct) Serr() string {
	if e.serr != nil {
		return *e.serr
	}
	return e.err
}

var tests = []struct {
	sql string
	r   *errorStruct
}{
	{ // 0
		sql: `SELECT count(*) FROM src1 HAVING sin(temp) > 0.3`,
		r:   newErrorStruct("Not allowed to call non-aggregate functions in HAVING clause."),
	},
	{ // 1
		sql: `SELECT count(*) FROM src1 WHERE name = "dname" HAVING sin(count(*)) > 0.3`,
		r:   newErrorStruct(""),
	},
	{ // 2
		sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sin(c) > 0.3`,
		r:   newErrorStruct(""),
	},
	{ // 3
		sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sum(c) > 0.3`,
		r:   newErrorStruct("invalid argument for func sum: aggregate argument is not allowed"),
	},
	{ // 4
		sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" GROUP BY sin(c)`,
		r:   newErrorStruct("Not allowed to call aggregate functions in GROUP BY clause."),
	},
	{ // 5
		sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sum(c) > 0.3 OR sin(temp) > 3`,
		r:   newErrorStruct("Not allowed to call non-aggregate functions in HAVING clause."),
	},
	{ // 6
		sql: `SELECT collect(*) as c FROM src1 WHERE name = "dname" HAVING c[2]->temp > 20 AND sin(c[0]->temp) > 0`,
		r:   newErrorStruct(""),
	},
	{ // 7
		sql: `SELECT collect(*) as c FROM src1 WHERE name = "dname" HAVING c[2]->temp + temp > 0`,
		r:   newErrorStruct("Not allowed to call non-aggregate functions in HAVING clause."),
	},
	{ // 8
		sql: `SELECT deduplicate(temp, true) as de FROM src1 HAVING cardinality(de) > 20`,
		r:   newErrorStruct(""),
	},
	{ // 9
		sql: `SELECT sin(temp) as temp FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 10
		sql: `SELECT sum(temp) as temp, count(temp) as temp FROM src1`,
		r:   newErrorStruct("duplicate alias temp"),
	},
	{ // 11
		sql: `SELECT sum(temp) as temp, count(temp) as ct FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 12
		sql: `SELECT collect(*)->abc FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 13
		sql: `SELECT sin(temp) as temp1, cos(temp1) FROM src1`,
		r:   newErrorStructWithS("unknown field temp1", ""),
	},
}

func Test_validation(t *testing.T) {
	store := kv.GetDefaultKVStore(path.Join(DbDir, "stream"))
	err := store.Open()
	if err != nil {
		t.Error(err)
		return
	}
	defer store.Close()
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
	}
	types := map[string]ast.StreamType{
		"src1": ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		store.Set(name, string(s))
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(store, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("%d. %q: error compile sql: %s\n", i, tt.sql, err)
			continue
		}
		_, err = createLogicalPlan(stmt, &api.RuleOption{
			IsEventTime:        false,
			LateTol:            0,
			Concurrency:        0,
			BufferLength:       0,
			SendMetaToSink:     false,
			Qos:                0,
			CheckpointInterval: 0,
			SendError:          true,
		}, store)
		if !reflect.DeepEqual(tt.r.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.sql, tt.r.err, err)
		}
	}
}

func Test_validationSchemaless(t *testing.T) {
	store := kv.GetDefaultKVStore(path.Join(DbDir, "stream"))
	err := store.Open()
	if err != nil {
		t.Error(err)
		return
	}
	defer store.Close()
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
	}
	types := map[string]ast.StreamType{
		"src1": ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		store.Set(name, string(s))
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(store, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("%d. %q: error compile sql: %s\n", i, tt.sql, err)
			continue
		}
		_, err = createLogicalPlan(stmt, &api.RuleOption{
			IsEventTime:        false,
			LateTol:            0,
			Concurrency:        0,
			BufferLength:       0,
			SendMetaToSink:     false,
			Qos:                0,
			CheckpointInterval: 0,
			SendError:          true,
		}, store)
		serr := tt.r.Serr()
		if !reflect.DeepEqual(serr, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.sql, serr, err)
		}
	}
}
