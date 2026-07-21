// Copyright 2026 EMQ Technologies Co., Ltd.
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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSafeDynamicFieldName(t *testing.T) {
	tests := []struct {
		identifier string
		want       bool
	}{
		{"events", true},
		{"MY_TABLE", true},
		{"_private1", true},
		{"", false},
		{"1column", false},
		{"my-column", false},
		{"a); DROP TABLE secret;--", false},
		{`"quoted"`, false},
		{"`quoted`", false},
		{"[quoted]", false},
	}
	for _, tt := range tests {
		t.Run(tt.identifier, func(t *testing.T) {
			if got := isSafeDynamicFieldName(tt.identifier); got != tt.want {
				t.Errorf("isSafeDynamicFieldName(%q) = %v, want %v", tt.identifier, got, tt.want)
			}
		})
	}
}

func TestExtractKeys(t *testing.T) {
	t.Run("explicit fields preserve database syntax", func(t *testing.T) {
		fields := []string{"[Order]", `"MixedCase"`, "`sensor-value`"}
		sink := &SQLSinkConnector{config: &sqlSinkConfig{Fields: fields}}
		got, err := sink.extractKeys(map[string]any{"ignored": 1})
		if err != nil {
			t.Fatalf("extractKeys() unexpected error: %v", err)
		}
		if !reflect.DeepEqual(got, fields) {
			t.Errorf("extractKeys() = %v, want %v", got, fields)
		}
	})

	t.Run("safe dynamic fields", func(t *testing.T) {
		sink := &SQLSinkConnector{config: &sqlSinkConfig{}}
		got, err := sink.extractKeys(map[string]any{"temperature": 20, "Sensor_2": 21})
		if err != nil {
			t.Fatalf("extractKeys() unexpected error: %v", err)
		}
		sort.Strings(got)
		want := []string{"Sensor_2", "temperature"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("extractKeys() = %v, want %v", got, want)
		}
	})

	invalid := []string{
		"sensor-value",
		"1column",
		"field name",
		`"x") VALUES ('safe'); DROP TABLE secret;--"`,
		"`x`) VALUES ('safe'); DROP TABLE secret;--`",
	}
	for _, key := range invalid {
		t.Run("reject "+key, func(t *testing.T) {
			sink := &SQLSinkConnector{config: &sqlSinkConfig{}}
			got, err := sink.extractKeys(map[string]any{"safe": 1, key: 2})
			if err == nil {
				t.Fatalf("extractKeys() accepted unsafe dynamic key %q", key)
			}
			if got != nil {
				t.Errorf("extractKeys() returned partial keys %v on error", got)
			}
			if !strings.Contains(err.Error(), strconv.Quote(key)) {
				t.Errorf("extractKeys() error %q does not identify key %q", err, key)
			}
		})
	}
}

func TestCollectRejectsDynamicIdentifierInjection(t *testing.T) {
	sink := &SQLSinkConnector{config: &sqlSinkConfig{Table: "events"}}
	ctx := mockContext.NewMockContext("TestCollectRejectsDynamicIdentifierInjection", "sqlSink")
	payload := `"x") VALUES ('safe'); DROP TABLE secret;--"`
	err := sink.collect(ctx, map[string]any{payload: "ignored"})
	if err == nil {
		t.Fatal("collect() accepted an injectable dynamic field name")
	}
	if !strings.Contains(err.Error(), strconv.Quote(payload)) {
		t.Errorf("collect() error %q does not identify rejected field", err)
	}
}

func TestSaveRejectsDynamicIdentifierInjection(t *testing.T) {
	ctx := mockContext.NewMockContext("TestSaveRejectsDynamicIdentifierInjection", "sqlSink")
	payload := `"x") VALUES ('safe'); DROP TABLE secret;--"`
	for _, rowkind := range []string{ast.RowkindInsert, ast.RowkindUpdate, ast.RowkindDelete} {
		t.Run(rowkind, func(t *testing.T) {
			sink := &SQLSinkConnector{config: &sqlSinkConfig{
				Table:        "events",
				RowKindField: "rowkind",
				KeyField:     "id",
			}}
			err := sink.save(ctx, "events", map[string]any{
				"rowkind": rowkind,
				"id":      1,
				payload:   "ignored",
			})
			if err == nil {
				t.Fatalf("save() accepted an injectable dynamic field name for %s", rowkind)
			}
			if !strings.Contains(err.Error(), strconv.Quote(payload)) {
				t.Errorf("save() error %q does not identify rejected field", err)
			}
		})
	}
}

func TestSQLBuildersPreserveConfiguredIdentifiers(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "sqlserver insert",
			got:  buildInsertSQL("[dbo].[Events]", []string{"[Order]", "[Value]"}, []string{"('1','x')"}),
			want: "INSERT INTO [dbo].[Events] ([Order],[Value]) values ('1','x');",
		},
		{
			name: "oracle insert",
			got:  buildInsertSQL(`"MixedCase"@remote`, []string{`"Order"`}, []string{"('x')"}),
			want: `INSERT INTO "MixedCase"@remote ("Order") values ('x');`,
		},
		{
			name: "mysql insert",
			got:  buildInsertSQL("`audit-log`", []string{"`sensor-value`"}, []string{"('1')"}),
			want: "INSERT INTO `audit-log` (`sensor-value`) values ('1');",
		},
		{
			name: "sqlserver update",
			got:  buildUpdateSQL("[dbo].[Events]", []string{"[Value]"}, []string{"'x'"}, "[ID]", "O'Brien"),
			want: "UPDATE [dbo].[Events] SET [Value]='x' WHERE [ID] = 'O''Brien';",
		},
		{
			name: "sqlserver delete",
			got:  buildDeleteSQL("[audit.v1]", "[ID]", 7),
			want: "DELETE FROM [audit.v1] WHERE [ID] = 7;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("SQL builder = %q, want %q", tt.got, tt.want)
			}
		})
	}
}
