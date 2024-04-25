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

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/pkg/store"
)

func TestQueryGenerator_SqlQueryStatement(t *testing.T) {
	type fields struct {
		indexSlice          []interface{}
		InternalSqlQueryCfg *InternalSqlQueryCfg
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "int index",
			fields: fields{
				indexSlice: nil,
				InternalSqlQueryCfg: &InternalSqlQueryCfg{
					Table:                    "table",
					Limit:                    2,
					IndexFieldName:           "responseTime",
					IndexFieldValue:          10,
					IndexFieldDataType:       "",
					IndexFieldDateTimeFormat: "",
				},
			},
			want:    "select top 2 * from table where responseTime > '10' order by responseTime ASC",
			wantErr: false,
		},
		{
			name: "time string index",
			fields: fields{
				indexSlice: nil,
				InternalSqlQueryCfg: &InternalSqlQueryCfg{
					Table:                    "table",
					Limit:                    2,
					IndexFieldName:           "responseTime",
					IndexFieldValue:          "2022-04-13 06:22:32.233",
					IndexFieldDataType:       "DATETIME",
					IndexFieldDateTimeFormat: "YYYY-MM-dd HH:mm:ssSSS",
				},
			},
			want:    "select top 2 * from table where responseTime > '2022-04-13 06:22:32.233' order by responseTime ASC",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt.fields.InternalSqlQueryCfg.InitIndexFieldStore()
		t.Run(tt.name, func(t *testing.T) {
			q := &SqlServerQueryGenerator{
				InternalSqlQueryCfg: tt.fields.InternalSqlQueryCfg,
			}
			got, err := q.SqlQueryStatement()
			if (err != nil) != tt.wantErr {
				t.Errorf("SqlQueryStatement() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SqlQueryStatement() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOracleQuery(t *testing.T) {
	cfg := &InternalSqlQueryCfg{
		Table: "t",
		Limit: 1,
	}
	cfg.InitIndexFieldStore()
	s := NewOracleQueryGenerate(cfg)
	query, err := s.SqlQueryStatement()
	require.NoError(t, err)
	require.Equal(t, query, "select * from (select * from t ) where rownum <= 1")
}

func TestInternalQuery(t *testing.T) {
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
		"internalSqlQueryCfg": tempCfg2,
	}
	sqlcfg2 := &sqlConfig{}
	err := sqlcfg2.Init(props2)
	require.Error(t, err)

	cfg := &InternalSqlQueryCfg{
		Table:           "table",
		Limit:           10,
		IndexFieldName:  "responseTime",
		IndexFieldValue: 10,
	}
	cfg.InitIndexFieldStore()
	s := NewSqlServerQuery(cfg)

	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": 20,
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": 30,
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": 40,
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": 50,
	})

	nextSqlStr, _ := s.SqlQueryStatement()

	want := "select top 10 * from table where responseTime > '50' order by responseTime ASC"

	if !reflect.DeepEqual(nextSqlStr, want) {
		t.Errorf("SqlQueryStatement() = %v, want %v", nextSqlStr, want)
	}
}

func TestGenerateSQLWithMultiIndex(t *testing.T) {
	testcases := []struct {
		cfg *InternalSqlQueryCfg
		sql string
	}{
		{
			cfg: &InternalSqlQueryCfg{
				Table: "t",
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
			sql: `select * from t where col1 > '1' AND col2 > '2' order by 'col1' ASC, 'col2' ASC`,
		},
		{
			cfg: &InternalSqlQueryCfg{
				Table: "t",
				store: store.NewIndexFieldWrap(
					&store.IndexField{
						IndexFieldName:  "col2",
						IndexFieldValue: 2,
					},
					&store.IndexField{
						IndexFieldName:  "col1",
						IndexFieldValue: 1,
					}),
			},
			sql: `select * from t where col2 > '2' AND col1 > '1' order by 'col2' ASC, 'col1' ASC`,
		},
		{
			cfg: &InternalSqlQueryCfg{
				Table: "t",
				Limit: 3,
				store: store.NewIndexFieldWrap(
					&store.IndexField{
						IndexFieldName:  "col2",
						IndexFieldValue: 2,
					},
					&store.IndexField{
						IndexFieldName:  "col1",
						IndexFieldValue: 1,
					}),
			},
			sql: `select * from t where col2 > '2' AND col1 > '1' order by 'col2' ASC, 'col1' ASC limit 3`,
		},
		{
			cfg: &InternalSqlQueryCfg{
				Table: "t",
				Limit: 3,
				store: store.NewIndexFieldWrap(),
			},
			sql: `select * from t  limit 3`,
		},
	}

	for _, tc := range testcases {
		g := NewCommonSqlQuery(tc.cfg)
		s, err := g.SqlQueryStatement()
		require.NoError(t, err)
		require.Equal(t, tc.sql, s)
	}
}
