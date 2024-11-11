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

package sql

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"

	"github.com/lf-edge/ekuiper/pkg/api"
)

func scanIntoMap(mapValue map[string]interface{}, values []interface{}, columns []string) {
	for idx, column := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			mapValue[column] = reflectValue.Interface()
			if valuer, ok := mapValue[column].(driver.Valuer); ok {
				mapValue[column], _ = valuer.Value()
			} else if b, ok := mapValue[column].(sql.RawBytes); ok {
				mapValue[column] = string(b)
			}
		} else {
			mapValue[column] = nil
		}
	}
}

func prepareValues(ctx api.StreamContext, values []interface{}, columnTypes []*sql.ColumnType, columns []string) {
	if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
			nullable, ok := columnType.Nullable()
			if got := buildScanValueByColumnType(ctx, columnType.Name(), columnType.DatabaseTypeName(), nullable && ok); got != nil {
				values[idx] = got
				continue
			}
			if columnType.ScanType() != nil {
				values[idx] = reflect.New(reflect.PtrTo(columnType.ScanType())).Interface()
			} else {
				values[idx] = new(interface{})
			}
		}
	} else {
		for idx := range columns {
			values[idx] = new(interface{})
		}
	}
}

func buildScanValueByColumnType(ctx api.StreamContext, colName, colType string, nullable bool) interface{} {
	switch strings.ToUpper(colType) {
	case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "TEXT", "NTEXT":
		if nullable {
			return &sql.NullString{}
		}
		return new(string)
	case "DECIMAL", "NUMERIC", "FLOAT", "REAL":
		if nullable {
			return &sql.NullFloat64{}
		}
		return new(float64)
	case "BOOL":
		if nullable {
			return &sql.NullBool{}
		}
		return new(bool)
	case "INT", "BIGINT", "SMALLINT", "TINYINT":
		if nullable {
			return &sql.NullInt64{}
		}
		return new(int64)
	default:
		ctx.GetLogger().Infof("sql source meet column %v unknown columnType:%v", colName, colType)
		return nil
	}
}
