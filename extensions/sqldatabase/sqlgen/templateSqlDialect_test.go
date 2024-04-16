// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package sqlgen

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/pkg/store"
)

func Test_templateSqlQueryCfg_getSqlQueryStatement(t1 *testing.T) {
	type fields struct {
		TemplateSql string
		FieldName   string
		FieldValue  interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   error
		want1  string
	}{
		{
			name: "select * from table",
			fields: fields{
				TemplateSql: "select * from table",
				FieldName:   "",
				FieldValue:  nil,
			},
			want:  nil,
			want1: "select * from table",
		},

		{
			name: "select * from table where id > {{.id}} ",
			fields: fields{
				TemplateSql: "select * from table where id > {{.id}}",
				FieldName:   "id",
				FieldValue:  100,
			},
			want:  nil,
			want1: "select * from table where id > 100",
		},

		{
			name: "select * from table where responseTime > `{{.responseTime}}` ",
			fields: fields{
				TemplateSql: "select * from table where responseTime > `{{.responseTime}}`",
				FieldName:   "responseTime",
				FieldValue:  "2008-10-29 14:56:59",
			},
			want:  nil,
			want1: "select * from table where responseTime > `2008-10-29 14:56:59`",
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			cfg := &TemplateSqlQueryCfg{
				TemplateSql:     tt.fields.TemplateSql,
				IndexFieldName:  tt.fields.FieldName,
				IndexFieldValue: tt.fields.FieldValue,
			}
			cfg.InitIndexFieldStore()
			query, _ := NewTemplateSqlQuery(cfg)

			got1, got := query.SqlQueryStatement()
			if !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("SqlQueryStatement() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t1.Errorf("SqlQueryStatement() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func getDatetimeFromstring(dateStr string) time.Time {
	myDate, _ := time.Parse("2006-01-02 15:04:05", dateStr)
	return myDate
}

func TestTemplateQuery(t *testing.T) {
	cfg := &TemplateSqlQueryCfg{
		TemplateSql:              "select * from table where responseTime > `{{.responseTime}}`",
		IndexFieldName:           "responseTime",
		IndexFieldValue:          getDatetimeFromstring("2008-10-25 14:56:59"),
		IndexFieldDataType:       DATETIME_TYPE,
		IndexFieldDateTimeFormat: "YYYY-MM-dd HH:mm:ssSSS",
	}
	cfg.InitIndexFieldStore()

	s, _ := NewTemplateSqlQuery(cfg)

	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-10-29 14:56:59"),
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-11-11 11:12:01"),
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-11-09 15:45:21"),
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-11-11 13:23:44"),
	})

	nextSqlStr, _ := s.SqlQueryStatement()

	want := "select * from table where responseTime > `2008-11-11 13:23:44.000`"

	if !reflect.DeepEqual(nextSqlStr, want) {
		t.Errorf("SqlQueryStatement() = %v, want %v", nextSqlStr, want)
	}
}

func TestTemplateQuery_DateTime(t *testing.T) {
	tempCfg := map[string]interface{}{
		"templateSql":    "select * from table where responseTime > `{{.responseTime}}`",
		"indexField":     "responseTime",
		"indexValue":     "2008-10-25 14:56:59.123",
		"indexFieldType": DATETIME_TYPE,
		"dateTimeFormat": "YYYY-MM-dd HH:mm:ssSSS",
	}

	props := map[string]interface{}{
		"templateSqlQueryCfg": tempCfg,
	}
	tempCfg2 := map[string]interface{}{
		"templateSql":    "select * from table where responseTime > `{{.responseTime}}`",
		"indexField":     "responseTime",
		"indexValue":     "2008-10-25 14:56:59.123",
		"indexFieldType": DATETIME_TYPE,
		"dateTimeFormat": "YYYY-MM-dd HH:mm:ssSSS",
		"indexFields": []map[string]interface{}{
			{
				"indexField":     "responseTime2",
				"indexValue":     "2008-10-25 14:56:59.123",
				"indexFieldType": DATETIME_TYPE,
				"dateTimeFormat": "YYYY-MM-dd HH:mm:ssSSS",
			},
		},
	}
	props2 := map[string]interface{}{
		"templateSqlQueryCfg": tempCfg2,
	}
	sqlcfg2 := &sqlConfig{}
	err := sqlcfg2.Init(props2)
	require.Error(t, err)

	sqlcfg := &sqlConfig{}
	_ = sqlcfg.Init(props)

	s, _ := NewTemplateSqlQuery(sqlcfg.TemplateSqlQueryCfg)

	// query
	firstSqlStr, _ := s.SqlQueryStatement()

	want := "select * from table where responseTime > `2008-10-25 14:56:59.123`"

	if !reflect.DeepEqual(firstSqlStr, want) {
		t.Errorf("SqlQueryStatement() = %v, want %v", firstSqlStr, want)
	}

	// query result
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-10-29 14:56:59"),
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-11-11 11:12:01"),
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-11-09 15:45:21"),
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-11-11 13:23:44"),
	})

	nextSqlStr, _ := s.SqlQueryStatement()

	want = "select * from table where responseTime > `2008-11-11 13:23:44.000`"

	if !reflect.DeepEqual(nextSqlStr, want) {
		t.Errorf("SqlQueryStatement() = %v, want %v", nextSqlStr, want)
	}
}

func TestGenerateTemplateWithMultiIndex(t *testing.T) {
	testcases := []struct {
		cfg *TemplateSqlQueryCfg
		sql string
	}{
		{
			cfg: &TemplateSqlQueryCfg{
				TemplateSql: "select * from table where col1 > `{{.col1}}` AND col2 > `{{.col2}}` order by co1 ASC, co2 ASC",
				store: store.NewIndexFieldWrap(
					&store.IndexField{
						IndexFieldName:  "col1",
						IndexFieldValue: 1,
					},
					&store.IndexField{
						IndexFieldName:  "col2",
						IndexFieldValue: 2,
					}),
			},
			sql: "select * from table where col1 > `1` AND col2 > `2` order by co1 ASC, co2 ASC",
		},
		{
			cfg: &TemplateSqlQueryCfg{
				TemplateSql: "select * from table",
				store:       store.NewIndexFieldWrap(),
			},
			sql: "select * from table",
		},
	}

	for _, tc := range testcases {
		tc.cfg.store.LoadFromList()
		g, err := NewTemplateSqlQuery(tc.cfg)
		require.NoError(t, err)
		s, err := g.SqlQueryStatement()
		require.NoError(t, err)
		require.Equal(t, tc.sql, s)
	}
}
