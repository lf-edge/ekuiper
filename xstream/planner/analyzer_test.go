package planner

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/kv"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"path"
	"reflect"
	"strings"
	"testing"
)

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
	types := map[string]xsql.StreamType{
		"src1": xsql.TypeStream,
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
	streams := make(map[string]*xsql.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(store, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}
	var tests = []struct {
		sql string
		err string
	}{
		{ // 0
			sql: `SELECT count(*) FROM src1 HAVING sin(temp) > 0.3`,
			err: "Not allowed to call non-aggregate functions in HAVING clause.",
		},
		{ // 1
			sql: `SELECT count(*) FROM src1 WHERE name = "dname" HAVING sin(count(*)) > 0.3`,
			err: "",
		},
		{ // 2
			sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sin(c) > 0.3`,
			err: "",
		},
		{ // 3
			sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sum(c) > 0.3`,
			err: "invalid argument for func sum: aggregate argument is not allowed",
		},
		{ // 4
			sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" GROUP BY sin(c)`,
			err: "Not allowed to call aggregate functions in GROUP BY clause.",
		},
		{ // 5
			sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sum(c) > 0.3 OR sin(temp) > 3`,
			err: "Not allowed to call non-aggregate functions in HAVING clause.",
		},
		{ // 6
			sql: `SELECT collect(*) as c FROM src1 WHERE name = "dname" HAVING c[2]->temp > 20 AND sin(c[0]->temp) > 0`,
			err: "",
		},
		{ // 7
			sql: `SELECT collect(*) as c FROM src1 WHERE name = "dname" HAVING c[2]->temp + temp > 0`,
			err: "Not allowed to call non-aggregate functions in HAVING clause.",
		},
		{ // 8
			sql: `SELECT deduplicate(temp, true) as de FROM src1 HAVING cardinality(de) > 20`,
			err: "",
		},
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
		if !reflect.DeepEqual(tt.err, common.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.sql, tt.err, err)
		}
	}
}
