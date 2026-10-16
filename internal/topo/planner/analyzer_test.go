// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package planner

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func init() {
	testx.InitEnv("planner")
	processor.NewStreamProcessor()
}

type errorStruct struct {
	err  string
	serr *string
}

func newErrorStruct(err string) *errorStruct {
	return &errorStruct{
		err: err,
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
		r:   newErrorStruct("Not allowed to call non-aggregate functions in HAVING clause: binaryExpr:{ Call:{ name:sin, args:[src1.temp] } > 0.3 }."),
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
		r:   newErrorStruct("Not allowed to call aggregate functions in GROUP BY clause: Call:{ name:sin, args:[$$alias.c,aliasRef:Call:{ name:count, args:[*] }] }."),
	},
	{ // 5
		sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sum(c) > 0.3 OR sin(temp) > 3`,
		r:   newErrorStruct("Not allowed to call non-aggregate functions in HAVING clause: binaryExpr:{ binaryExpr:{ Call:{ name:sum, args:[$$alias.c,aliasRef:Call:{ name:count, args:[*] }] } > 0.3 } OR binaryExpr:{ Call:{ name:sin, args:[src1.temp] } > 3 } }."),
	},
	{ // 6
		sql: `SELECT collect(*) as c FROM src1 WHERE name = "dname" HAVING c[2]->temp > 20 AND sin(c[0]->temp) > 0`,
		r:   newErrorStruct(""),
	},
	{ // 7
		sql: `SELECT collect(*) as c FROM src1 WHERE name = "dname" HAVING c[2]->temp + temp > 0`,
		r:   newErrorStruct("Not allowed to call non-aggregate functions in HAVING clause: binaryExpr:{ binaryExpr:{ binaryExpr:{ binaryExpr:{ $$alias.c,aliasRef:Call:{ name:collect, args:[*] }[2] } -> jsonFieldName:temp } + src1.temp } > 0 }."),
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
		sql: `SELECT count(temp) as temp, sum(temp) as temp1 FROM src1`,
		r:   newErrorStruct("invalid argument for func sum: aggregate argument is not allowed"),
	},
	{ // 11
		sql: `SELECT sum(temp) as temp1, count(temp) as ct FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 12
		sql: `SELECT collect(*)->abc FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 13
		sql: `SELECT sin(temp) as temp1, cos(temp1) FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 14
		sql: `SELECT collect(*)[-1] as current FROM src1 GROUP BY COUNTWINDOW(2, 1) HAVING isNull(current->name) = false`,
		r:   newErrorStruct(""),
	},
	{ // 15
		sql: `SELECT sum(next->nid) as nid FROM src1 WHERE next->nid > 20 `,
		r:   newErrorStruct(""),
	},
	{ // 16
		sql: `SELECT collect(*)[0] as last FROM src1 GROUP BY SlidingWindow(ss,5) HAVING last.temp > 30`,
		r:   newErrorStruct(""),
	},
	{ // 17
		sql: `SELECT last_hit_time() FROM src1 GROUP BY SlidingWindow(ss,5) HAVING last_agg_hit_count() < 3`,
		r:   newErrorStruct("function last_hit_time is not allowed in an aggregate query"),
	},
	{ // 18
		sql: `SELECT * FROM src1 GROUP BY SlidingWindow(ss,5) Over (WHEN last_hit_time() > 1) HAVING last_agg_hit_count() < 3`,
		r:   newErrorStruct(""),
	},
	{
		sql: "select a + 1 as b, b + 1 as a from src1",
		r:   newErrorStruct("select fields have cycled alias"),
	},
	{
		sql: "select a + 1 as b, b * 2 as c, c + 1 as a from src1",
		r:   newErrorStruct("select fields have cycled alias"),
	},
	//{ // 19 already captured in parser
	//	sql: `SELECT * FROM src1 GROUP BY SlidingWindow(ss,5) Over (WHEN abs(sum(a)) > 1) HAVING last_agg_hit_count() < 3`,
	//	r:   newErrorStruct("error compile sql: Not allowed to call aggregate functions in GROUP BY clause."),
	//},
}

func TestCheckTopoSort(t *testing.T) {
	store, err := store.GetKV("stream")
	require.NoError(t, err)
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string,
					next STRUCT(NAME STRING, NID BIGINT)
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
		require.NoError(t, err)
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
	sql := "select latest(a) as a from src1"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	_, err = CreateLogicalPlan(stmt, &def.RuleOption{
		IsEventTime:        false,
		LateTol:            0,
		Concurrency:        0,
		BufferLength:       0,
		SendMetaToSink:     false,
		Qos:                0,
		CheckpointInterval: 0,
		SendError:          true,
	}, store)
	require.EqualError(t, err, "unknown field a")
}

func TestLeadUntilPlan(t *testing.T) {
	kvStore, err := store.GetKV("stream")
	require.NoError(t, err)
	streamSQL := `CREATE STREAM lead_src () WITH (DATASOURCE="lead_src", FORMAT="json");`
	encoded, err := json.Marshal(&xsql.StreamInfo{StreamType: ast.TypeStream, Statement: streamSQL})
	require.NoError(t, err)
	require.NoError(t, kvStore.Set("lead_src", string(encoded)))

	sql := `SELECT ts AS input_ts INVISIBLE,
		latest(input_ts) OVER (WHEN a < 5 AND isNull(b)) AS candidate_t2 INVISIBLE,
		lead(candidate_t2) OVER (WHEN isNull(b) = false UNTIL ts - current_row(ts) > 5) AS t2
		FROM lead_src`
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	plan, err := CreateLogicalPlan(stmt, &def.RuleOption{SendError: true}, kvStore)
	require.NoError(t, err)
	var analytic *AnalyticFuncsPlan
	var findAnalytic func(LogicalPlan)
	findAnalytic = func(current LogicalPlan) {
		if value, ok := current.(*AnalyticFuncsPlan); ok {
			analytic = value
			return
		}
		for _, child := range current.Children() {
			findAnalytic(child)
		}
	}
	findAnalytic(plan)
	require.NotNil(t, analytic)
	var leadCall *ast.Call
	for _, call := range append(analytic.funcs, analytic.fieldFuncs...) {
		if call.Name == "lead" {
			leadCall = call
		}
	}
	require.NotNil(t, leadCall)
	aliasArg, ok := leadCall.Args[0].(*ast.FieldRef)
	require.True(t, ok)
	require.NotNil(t, aliasArg.AliasRef)
	latest, ok := aliasArg.AliasRef.Expression.(*ast.Call)
	require.True(t, ok)
	require.Equal(t, "latest", latest.Name)
	require.True(t, latest.Cached)
	inputAlias, ok := latest.Args[0].(*ast.FieldRef)
	require.True(t, ok)
	require.True(t, inputAlias.IsAlias())
	require.NotNil(t, inputAlias.AliasRef)
	rawInput, ok := inputAlias.AliasRef.Expression.(*ast.FieldRef)
	require.True(t, ok)
	require.Equal(t, ast.StreamName("lead_src"), rawInput.StreamName)
	require.Equal(t, "ts", rawInput.Name)

	for _, invalid := range []string{
		`SELECT latest(lead(ts)) FROM lead_src`,
		`SELECT lead(lead(ts)) FROM lead_src`,
	} {
		stmt, err = xsql.NewParser(strings.NewReader(invalid)).Parse()
		require.NoError(t, err)
		_, err = CreateLogicalPlan(stmt, &def.RuleOption{SendError: true}, kvStore)
		require.ErrorContains(t, err, "lead output cannot feed")
	}
}

func TestSchemalessDirectFieldAliasBinding(t *testing.T) {
	kvStore, err := store.GetKV("stream")
	require.NoError(t, err)
	streamSQL := `CREATE STREAM schemaless_alias_src () WITH (DATASOURCE="schemaless_alias_src", FORMAT="json");`
	encoded, err := json.Marshal(&xsql.StreamInfo{StreamType: ast.TypeStream, Statement: streamSQL})
	require.NoError(t, err)
	require.NoError(t, kvStore.Set("schemaless_alias_src", string(encoded)))

	sql := `SELECT temp AS input_temp INVISIBLE,
		CASE WHEN input_temp > 26 THEN 1 ELSE 0 END AS state_condition INVISIBLE,
		latest(state_condition, 0) AS state
		FROM schemaless_alias_src`
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	plan, err := CreateLogicalPlan(stmt, &def.RuleOption{SendError: true}, kvStore)
	require.NoError(t, err)

	var project *ProjectPlan
	var findProject func(LogicalPlan)
	findProject = func(current LogicalPlan) {
		if value, ok := current.(*ProjectPlan); ok {
			project = value
			return
		}
		for _, child := range current.Children() {
			findProject(child)
		}
	}
	findProject(plan)
	require.NotNil(t, project)

	var conditionAlias *ast.FieldRef
	for _, field := range project.fields {
		if field.AName == "state_condition" {
			conditionAlias, _ = field.Expr.(*ast.FieldRef)
			break
		}
	}
	require.NotNil(t, conditionAlias)
	require.True(t, conditionAlias.IsAlias())
	require.NotNil(t, conditionAlias.AliasRef)

	var inputAlias *ast.FieldRef
	ast.WalkFunc(conditionAlias.AliasRef.Expression, func(node ast.Node) bool {
		if field, ok := node.(*ast.FieldRef); ok && field.Name == "input_temp" {
			inputAlias = field
		}
		return true
	})
	require.NotNil(t, inputAlias)
	require.True(t, inputAlias.IsAlias())
	require.NotNil(t, inputAlias.AliasRef)
	rawInput, ok := inputAlias.AliasRef.Expression.(*ast.FieldRef)
	require.True(t, ok)
	require.Equal(t, ast.StreamName("schemaless_alias_src"), rawInput.StreamName)
	require.Equal(t, "temp", rawInput.Name)
}

func Test_validation(t *testing.T) {
	store, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string,
					next STRUCT(NAME STRING, NID BIGINT)
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
		_, err = CreateLogicalPlan(stmt, &def.RuleOption{
			IsEventTime:        false,
			LateTol:            0,
			Concurrency:        0,
			BufferLength:       0,
			SendMetaToSink:     false,
			Qos:                0,
			CheckpointInterval: 0,
			SendError:          true,
		}, store)
		assert.Equal(t, tt.r.err, testx.Errstring(err), tt.sql)
	}
}

func Test_validationSchemaless(t *testing.T) {
	store, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
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
		_, err = CreateLogicalPlan(stmt, &def.RuleOption{
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
		require.Equal(t, serr, testx.Errstring(err))
	}
}

func TestConvertStreamInfo(t *testing.T) {
	testCases := []struct {
		name       string
		streamStmt *ast.StreamStmt
		expected   ast.StreamFields
	}{
		{
			name: "with match fields & schema",
			streamStmt: &ast.StreamStmt{
				StreamFields: []ast.StreamField{
					{
						Name: "field1",
						FieldType: &ast.BasicType{
							Type: ast.BIGINT,
						},
					},
					{
						Name: "field2",
						FieldType: &ast.BasicType{
							Type: ast.STRINGS,
						},
					},
				},
				Options: &ast.Options{
					FORMAT:    "protobuf",
					SCHEMAID:  "myschema.schema1",
					TIMESTAMP: "ts",
				},
			},
			expected: []ast.StreamField{
				{
					Name: "field1",
					FieldType: &ast.BasicType{
						Type: ast.BIGINT,
					},
				},
				{
					Name: "field2",
					FieldType: &ast.BasicType{
						Type: ast.STRINGS,
					},
				},
			},
		},
		{
			name: "with unmatch fields & schema",
			streamStmt: &ast.StreamStmt{
				StreamFields: []ast.StreamField{
					{
						Name: "field1",
						FieldType: &ast.BasicType{
							Type: ast.STRINGS,
						},
					},
					{
						Name: "field2",
						FieldType: &ast.BasicType{
							Type: ast.STRINGS,
						},
					},
				},
				Options: &ast.Options{
					FORMAT:    "protobuf",
					SCHEMAID:  "myschema.schema1",
					TIMESTAMP: "ts",
				},
			},
			expected: []ast.StreamField{
				{
					Name: "field1",
					FieldType: &ast.BasicType{
						Type: ast.BIGINT,
					},
				},
				{
					Name: "field2",
					FieldType: &ast.BasicType{
						Type: ast.STRINGS,
					},
				},
			},
		},
		{
			name: "without schema",
			streamStmt: &ast.StreamStmt{
				StreamFields: []ast.StreamField{
					{
						Name: "field1",
						FieldType: &ast.BasicType{
							Type: ast.FLOAT,
						},
					},
					{
						Name: "field2",
						FieldType: &ast.BasicType{
							Type: ast.STRINGS,
						},
					},
				},
				Options: &ast.Options{
					FORMAT:    "json",
					TIMESTAMP: "ts",
				},
			},
			expected: []ast.StreamField{
				{
					Name: "field1",
					FieldType: &ast.BasicType{
						Type: ast.FLOAT,
					},
				},
				{
					Name: "field2",
					FieldType: &ast.BasicType{
						Type: ast.STRINGS,
					},
				},
			},
		},
		{
			name: "without fields",
			streamStmt: &ast.StreamStmt{
				Options: &ast.Options{
					FORMAT:    "protobuf",
					SCHEMAID:  "myschema.schema1",
					TIMESTAMP: "ts",
				},
			},
			expected: []ast.StreamField{
				{
					Name: "field1",
					FieldType: &ast.BasicType{
						Type: ast.BIGINT,
					},
				},
				{
					Name: "field2",
					FieldType: &ast.BasicType{
						Type: ast.STRINGS,
					},
				},
			},
		},
		{
			name: "schemaless",
			streamStmt: &ast.StreamStmt{
				Options: &ast.Options{
					FORMAT:    "json",
					TIMESTAMP: "ts",
				},
			},
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := convertStreamInfo(tc.streamStmt)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if !reflect.DeepEqual(actual.schema, tc.expected) {
				t.Errorf("unexpected result: got %v, want %v", actual.schema, tc.expected)
			}
		})
	}
}

func TestValidateStmt(t *testing.T) {
	store, err := store.GetKV("stream")
	require.NoError(t, err)
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string,
					next STRUCT(NAME STRING, NID BIGINT)
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
		require.NoError(t, err)
		store.Set(name, string(s))
	}
	sql := "select a from src1 group by b"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	err = validate(stmt)
	require.Error(t, err)
}

func TestAreTypesCompatible(t *testing.T) {
	testcases := []struct {
		left  string
		right string
		ok    bool
	}{
		{left: "bigint", right: "bigint", ok: true},
		{left: "float", right: "float", ok: true},
		{left: "bigint", right: "float", ok: true},
		{left: "float", right: "bigint", ok: true},
		{left: "string", right: "string", ok: true},
		{left: "boolean", right: "boolean", ok: true},
		{left: "datetime", right: "datetime", ok: true},
		{left: "bytea", right: "bytea", ok: true},
		{left: "array", right: "array", ok: true},
		{left: "struct", right: "struct", ok: true},
		{left: "string", right: "bigint", ok: false},
		{left: "float", right: "string", ok: false},
		{left: "boolean", right: "bigint", ok: false},
		{left: "datetime", right: "string", ok: false},
		{left: "bytea", right: "string", ok: false},
		{left: "array", right: "struct", ok: false},
		{left: "struct", right: "string", ok: false},
	}
	for _, tc := range testcases {
		require.Equal(t, tc.ok, areTypesCompatible(tc.left, tc.right), "%s vs %s", tc.left, tc.right)
	}
}

func TestGetFieldType(t *testing.T) {
	schema := map[string]*ast.JsonStreamField{
		"id":   {Type: "bigint"},
		"name": {Type: "string"},
	}
	testcases := []struct {
		name   string
		field  ast.Field
		schema map[string]*ast.JsonStreamField
		typ    string
	}{
		{
			name:   "field in schema",
			field:  ast.Field{Name: "id", Expr: &ast.FieldRef{StreamName: "s1", Name: "id"}},
			schema: schema,
			typ:    "bigint",
		},
		{
			name:   "field not in schema",
			field:  ast.Field{Name: "missing", Expr: &ast.FieldRef{StreamName: "s1", Name: "missing"}},
			schema: schema,
			typ:    "",
		},
		{
			name:   "schemaless stream",
			field:  ast.Field{Name: "id", Expr: &ast.FieldRef{StreamName: "s1", Name: "id"}},
			schema: nil,
			typ:    "",
		},
		{
			name: "alias of a field resolves to the aliased field",
			field: ast.Field{Name: "id", AName: "id", Expr: &ast.FieldRef{
				StreamName: ast.AliasStream,
				Name:       "id",
				AliasRef:   &ast.AliasRef{Expression: &ast.FieldRef{StreamName: "s1", Name: "name"}},
			}},
			schema: schema,
			typ:    "string",
		},
		{
			name: "alias of a function call is unknown",
			field: ast.Field{Name: "x", AName: "x", Expr: &ast.FieldRef{
				StreamName: ast.AliasStream,
				Name:       "x",
				AliasRef:   &ast.AliasRef{Expression: &ast.Call{Name: "abs", Args: []ast.Expr{&ast.FieldRef{StreamName: "s1", Name: "id"}}}},
			}},
			schema: schema,
			typ:    "",
		},
		{
			name:   "function call is unknown",
			field:  ast.Field{Name: "abs", Expr: &ast.Call{Name: "abs", Args: []ast.Expr{&ast.FieldRef{StreamName: "s1", Name: "id"}}}},
			schema: schema,
			typ:    "",
		},
		{
			name:   "literal is unknown",
			field:  ast.Field{Name: "kuiper_field_0", Expr: &ast.IntegerLiteral{Val: 1}},
			schema: schema,
			typ:    "",
		},
	}
	for _, tc := range testcases {
		require.Equal(t, tc.typ, getFieldType(tc.field, tc.schema), tc.name)
	}
}

func TestSetOperatorTypeCheck(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	streamSqls := map[string]string{
		"typedLeft": `CREATE STREAM typedLeft (
					id BIGINT,
					temp FLOAT,
					name STRING,
					ts DATETIME,
					tags ARRAY(STRING),
					info STRUCT(city STRING, zip BIGINT),
				) WITH (DATASOURCE="src1");`,
		"typedRight": `CREATE STREAM typedRight (
					id BIGINT,
					temp BIGINT,
					name STRING,
					online BOOLEAN,
					raw BYTEA,
					tags ARRAY(BIGINT),
					info STRUCT(n BIGINT),
				) WITH (DATASOURCE="src2");`,
		"schemalessRight": `CREATE STREAM schemalessRight () WITH (DATASOURCE="src3");`,
		"typedLeftCopy": `CREATE STREAM typedLeftCopy (
					id BIGINT,
					temp FLOAT,
					name STRING,
					ts DATETIME,
					tags ARRAY(STRING),
					info STRUCT(city STRING, zip BIGINT),
				) WITH (DATASOURCE="src4");`,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: ast.TypeStream,
			Statement:  sql,
		})
		require.NoError(t, err)
		require.NoError(t, kv.Set(name, string(s)))
	}

	testcases := []struct {
		sql string
		err string
	}{
		// compatible projections
		{
			sql: `SELECT id, name FROM typedLeft UNION SELECT id, name FROM typedRight`,
		},
		{
			sql: `SELECT temp FROM typedLeft UNION ALL SELECT temp FROM typedRight`,
		},
		{
			sql: `SELECT typedLeft.id, typedLeft.name FROM typedLeft UNION SELECT typedRight.id, typedRight.name FROM typedRight`,
		},
		{
			sql: `SELECT id AS key, name AS label FROM typedLeft INTERSECT SELECT temp AS key, name AS label FROM typedRight`,
		},
		{
			sql: `SELECT temp FROM typedLeft WHERE id > 3 AND name = "a" UNION SELECT temp FROM typedRight WHERE online = true`,
		},
		{
			sql: `SELECT id, temp, name, ts, tags, info FROM typedLeft UNION SELECT id, temp, name, ts, tags, info FROM typedLeftCopy`,
		},
		// unknown types are skipped
		{
			sql: `SELECT abs(temp), name FROM typedLeft UNION SELECT name, name FROM typedRight`,
		},
		{
			sql: `SELECT temp * 2 + 1, info->city FROM typedLeft UNION SELECT name, online FROM typedRight`,
		},
		{
			sql: `SELECT CASE WHEN temp > 1 THEN name ELSE "x" END, 1, "x" FROM typedLeft UNION SELECT online, name, id FROM typedRight`,
		},
		{
			sql: `SELECT name FROM typedLeft UNION SELECT whatever FROM schemalessRight`,
		},
		{
			sql: `SELECT upper(name) AS u FROM typedLeft UNION SELECT id FROM typedRight`,
		},
		// arrays and structs are only compared by their top-level type
		{
			sql: `SELECT tags, info FROM typedLeft UNION SELECT tags, info FROM typedRight`,
		},
		// incompatible projections
		{
			sql: `SELECT name FROM typedLeft INTERSECT SELECT id FROM typedRight`,
			err: "incompatible types in column 1 of set operator: name is string but id is bigint",
		},
		{
			sql: `SELECT id, temp FROM typedLeft EXCEPT SELECT id, online FROM typedRight`,
			err: "incompatible types in column 2 of set operator: temp is float but online is boolean",
		},
		{
			sql: `SELECT ts FROM typedLeft UNION SELECT name FROM typedRight`,
			err: "incompatible types in column 1 of set operator: ts is datetime but name is string",
		},
		{
			sql: `SELECT name FROM typedLeft UNION ALL SELECT raw FROM typedRight`,
			err: "incompatible types in column 1 of set operator: name is string but raw is bytea",
		},
		{
			sql: `SELECT tags FROM typedLeft UNION SELECT info FROM typedRight`,
			err: "incompatible types in column 1 of set operator: tags is array but info is struct",
		},
		{
			sql: `SELECT abs(temp), name, id, ts FROM typedLeft UNION SELECT temp, name, id, online FROM typedRight`,
			err: "incompatible types in column 4 of set operator: ts is datetime but online is boolean",
		},
		{
			sql: `SELECT typedLeft.name FROM typedLeft EXCEPT ALL SELECT typedRight.temp FROM typedRight`,
			err: "incompatible types in column 1 of set operator: name is string but temp is bigint",
		},
		// invisible fields are skipped, so columns are matched among the visible ones
		{
			sql: `SELECT id, ts INVISIBLE, name FROM typedLeft UNION SELECT id, name FROM typedRight`,
		},
		{
			sql: `SELECT id, ts INVISIBLE, temp FROM typedLeft UNION SELECT id, name FROM typedRight`,
			err: "incompatible types in column 2 of set operator: temp is float but name is string",
		},
		// aliases are resolved to the aliased field, not looked up by alias name
		{
			sql: `SELECT name AS id FROM typedLeft UNION SELECT name FROM typedRight`,
		},
		{
			sql: `SELECT name AS id FROM typedLeft UNION SELECT id FROM typedRight`,
			err: "incompatible types in column 1 of set operator: id is string but id is bigint",
		},
		{
			sql: `SELECT id AS a, a FROM typedLeft UNION SELECT id, name FROM typedRight`,
			err: "incompatible types in column 2 of set operator: a is bigint but name is string",
		},
		{
			sql: `SELECT name FROM typedLeft UNION SELECT online AS flag FROM typedRight`,
			err: "incompatible types in column 1 of set operator: name is string but flag is boolean",
		},
	}
	for _, tc := range testcases {
		stmt, err := xsql.NewParser(strings.NewReader(tc.sql)).Parse()
		require.NoError(t, err, tc.sql)
		_, err = CreateLogicalPlan(stmt, &def.RuleOption{
			SetOperatorWindowLength: 10,
			SetOperatorWindowUnit:   "SS",
		}, kv)
		if tc.err == "" {
			require.NoError(t, err, tc.sql)
		} else {
			require.EqualError(t, err, tc.err, tc.sql)
		}
	}
}
