// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
					Table:          "table",
					Limit:          2,
					IndexField:     "responseTime",
					IndexValue:     10,
					IndexFieldType: "",
					DateTimeFormat: "",
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
					Table:          "table",
					Limit:          2,
					IndexField:     "responseTime",
					IndexValue:     "2022-04-13 06:22:32.233",
					IndexFieldType: "DATETIME",
					DateTimeFormat: "YYYY-MM-dd HH:mm:ssSSS",
				},
			},
			want:    "select top 2 * from table where responseTime > '2022-04-13 06:22:32.233' order by responseTime ASC",
			wantErr: false,
		},
	}
	for _, tt := range tests {
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
	s := NewOracleQueryGenerate(&InternalSqlQueryCfg{
		Table: "t",
		Limit: 1,
	})
	query, err := s.SqlQueryStatement()
	require.NoError(t, err)
	require.Equal(t, query, "select * from (select * from t ) where rownum <= 1")
}

func TestInternalQuery(t *testing.T) {
	s := NewSqlServerQuery(&InternalSqlQueryCfg{
		Table:      "table",
		Limit:      10,
		IndexField: "responseTime",
		IndexValue: 10,
	})

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
