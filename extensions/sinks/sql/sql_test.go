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

package main

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"testing"

	econf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
)

func TestSingle(t *testing.T) {
	db, err := sql.Open("sqlite", "file:test.db")
	if err != nil {
		t.Error(err)
		return
	}
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	s := &sqlSink{}
	defer func() {
		db.Close()
		s.Close(ctx)
		err := os.Remove("test.db")
		if err != nil {
			fmt.Println(err)
		}
	}()
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS single (id BIGINT PRIMARY KEY, name TEXT NOT NULL, address varchar(20), mobile varchar(20))")
	if err != nil {
		panic(err)
	}
	err = s.Configure(map[string]interface{}{
		"url":   "sqlite://test.db",
		"table": "single",
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = s.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	var data = []map[string]interface{}{
		{"id": 1, "name": "John", "address": "343", "mobile": "334433"},
		{"id": 2, "name": "Susan", "address": "34", "mobile": "334433"},
		{"id": 3, "name": "Susan", "address": "34", "mobile": "334433"},
	}
	for _, d := range data {
		err = s.Collect(ctx, d)
		if err != nil {
			t.Error(err)
			return
		}
	}
	s.Close(ctx)
	rows, err := db.Query("SELECT * FROM single")
	if err != nil {
		t.Error(err)
		return
	}
	act, _ := rowsToMap(rows)
	exp := []map[string]interface{}{
		{"id": int64(1), "name": "John", "address": "343", "mobile": "334433"},
		{"id": int64(2), "name": "Susan", "address": "34", "mobile": "334433"},
		{"id": int64(3), "name": "Susan", "address": "34", "mobile": "334433"},
	}
	if !reflect.DeepEqual(act, exp) {
		t.Errorf("Expect %v but got %v", exp, act)
	}
}

func TestBatch(t *testing.T) {
	db, err := sql.Open("sqlite", "file:test.db")
	if err != nil {
		t.Error(err)
		return
	}
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	s := &sqlSink{}
	defer func() {
		db.Close()
		s.Close(ctx)
		err := os.Remove("test.db")
		if err != nil {
			fmt.Println(err)
		}
	}()
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS batch (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
	if err != nil {
		panic(err)
	}
	err = s.Configure(map[string]interface{}{
		"url":    "sqlite://test.db",
		"table":  "batch",
		"fields": []string{"id", "name"},
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = s.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	var data = []map[string]interface{}{
		{"id": 1, "name": "John", "address": "343", "mobile": "334433"},
		{"id": 2, "name": "Susan", "address": "34", "mobile": "334433"},
		{"id": 3, "name": "Susan", "address": "34", "mobile": "334433"},
	}
	err = s.Collect(ctx, data)
	if err != nil {
		t.Error(err)
		return
	}
	s.Close(ctx)
	rows, err := db.Query("SELECT * FROM batch")
	if err != nil {
		t.Error(err)
		return
	}
	act, _ := rowsToMap(rows)
	exp := []map[string]interface{}{
		{"id": int64(1), "name": "John"},
		{"id": int64(2), "name": "Susan"},
		{"id": int64(3), "name": "Susan"},
	}
	if !reflect.DeepEqual(act, exp) {
		t.Errorf("Expect %v but got %v", exp, act)
	}
}

func TestUpdate(t *testing.T) {
	db, err := sql.Open("sqlite", "file:test.db")
	if err != nil {
		t.Error(err)
		return
	}
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	s := &sqlSink{}
	defer func() {
		db.Close()
		s.Close(ctx)
		err := os.Remove("test.db")
		if err != nil {
			fmt.Println(err)
		}
	}()
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS updateTable (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
	if err != nil {
		panic(err)
	}
	err = s.Configure(map[string]interface{}{
		"url":          "sqlite://test.db",
		"table":        "updateTable",
		"rowkindField": "action",
		"keyField":     "id",
		"fields":       []string{"id", "name"},
	})
	if err != nil {
		t.Error(err)
		return
	}
	err = s.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	var test = []struct {
		d []map[string]interface{}
		b bool
		r []map[string]interface{}
	}{
		{
			d: []map[string]interface{}{
				{"id": 1, "name": "John", "address": "343", "mobile": "334433"},
				{"action": "insert", "id": 2, "name": "Susan", "address": "34", "mobile": "334433"},
				{"action": "update", "id": 2, "name": "Diana"},
			},
			b: true,
			r: []map[string]interface{}{
				{"id": int64(1), "name": "John"},
				{"id": int64(2), "name": "Diana"},
			},
		}, {
			d: []map[string]interface{}{
				{"id": 4, "name": "Charles", "address": "343", "mobile": "334433"},
				{"action": "delete", "id": 2},
				{"action": "update", "id": 1, "name": "Lizz"},
			},
			b: false,
			r: []map[string]interface{}{
				{"id": int64(1), "name": "Lizz"},
				{"id": int64(4), "name": "Charles"},
			},
		}, {
			d: []map[string]interface{}{
				{"action": "upsert", "id": 4, "name": "Charles", "address": "343", "mobile": "334433"},
				{"action": "update", "id": 3, "name": "Lizz"},
				{"action": "update", "id": 1, "name": "Philips"},
			},
			b: true,
			r: []map[string]interface{}{
				{"id": int64(1), "name": "Philips"},
				{"id": int64(4), "name": "Charles"},
			},
		},
	}
	for i, tt := range test {
		if tt.b {
			err = s.Collect(ctx, tt.d)
			if err != nil {
				fmt.Println(err)
			}
		} else {
			for _, d := range tt.d {
				err = s.Collect(ctx, d)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
		rows, err := db.Query("SELECT * FROM updateTable")
		if err != nil {
			t.Error(err)
			return
		}
		act, _ := rowsToMap(rows)
		if !reflect.DeepEqual(act, tt.r) {
			t.Errorf("Case %d Expect %v but got %v", i, tt.r, act)
		}
	}
}

func rowsToMap(rows *sql.Rows) ([]map[string]interface{}, error) {
	cols, _ := rows.Columns()

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var result []map[string]interface{}
	for rows.Next() {
		data := make(map[string]interface{})
		columns := make([]interface{}, len(cols))
		prepareValues(columns, types, cols)

		err := rows.Scan(columns...)
		if err != nil {
			return nil, err
		}
		scanIntoMap(data, columns, cols)
		result = append(result, data)
	}
	return result, nil
}

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

func prepareValues(values []interface{}, columnTypes []*sql.ColumnType, columns []string) {
	if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
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
