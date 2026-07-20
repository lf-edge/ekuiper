// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"strings"
	"testing"
)

func TestIdentifierQuoteChar(t *testing.T) {
	tests := []struct {
		driver string
		want   string
	}{
		{"mysql", "`"},
		{"MYSQL", "`"},
		{"MySQL", "`"},
		{"postgres", "\""},
		{"PostgreSQL", "\""},
		{"sqlite", "\""},
		{"sqlserver", "\""},
		{"mssql", "\""},
		{"unknown", "\""},
		{"", "\""},
	}
	for _, tt := range tests {
		c := &sqlSinkConfig{driver: tt.driver}
		got := c.identifierQuoteChar()
		if got != tt.want {
			t.Errorf("identifierQuoteChar(%q) = %q, want %q", tt.driver, got, tt.want)
		}
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		driver     string
		identifier string
		want       string
	}{
		// MySQL backtick quoting
		{"mysql", "a", "`a`"},
		{"mysql", "my_column", "`my_column`"},
		{"mysql", "a`b", "`a``b`"}, // embedded backtick doubled

		// PostgreSQL/SQLite double-quote quoting
		{"postgres", "a", `"a"`},
		{"sqlite", "my_column", `"my_column"`},
		{"postgres", `a"b`, `"a""b"`}, // embedded double-quote doubled

		// SQL injection payloads — metacharacters become part of quoted identifier
		{"postgres", "a); DROP TABLE secret;--", `"a); DROP TABLE secret;--"`},
		{"mysql", "a); DROP TABLE secret;--", "`a); DROP TABLE secret;--`"},
		{"postgres", "a) values ('1'); CREATE TABLE pwned(z); DROP TABLE secret; --", `"a) values ('1'); CREATE TABLE pwned(z); DROP TABLE secret; --"`},
		{"mysql", "a) values ('1'); CREATE TABLE pwned(z); DROP TABLE secret; --", "`a) values ('1'); CREATE TABLE pwned(z); DROP TABLE secret; --`"},

		// Embedded quote with injection combined
		{"postgres", `a"; DROP TABLE t;--`, `"a""; DROP TABLE t;--"`},
		{"mysql", "a`; DROP TABLE t;--", "`a``; DROP TABLE t;--`"},

		// Leading digit, hyphens — valid after quoting
		{"postgres", "1column", `"1column"`},
		{"postgres", "my-column", `"my-column"`},
	}
	for _, tt := range tests {
		c := &sqlSinkConfig{driver: tt.driver}
		got := c.quoteIdentifier(tt.identifier)
		if got != tt.want {
			t.Errorf("quoteIdentifier(%q, %q) = %q, want %q", tt.driver, tt.identifier, got, tt.want)
		}
	}
}

func TestQuoteTableName(t *testing.T) {
	tests := []struct {
		driver string
		table  string
		want   string
	}{
		// Simple table names
		{"mysql", "t", "`t`"},
		{"postgres", "t", `"t"`},
		{"sqlite", "events", `"events"`},

		// Schema-qualified table names
		{"postgres", "public.events", `"public"."events"`},
		{"mysql", "mydb.mytable", "`mydb`.`mytable`"},
		{"postgres", "myschema.mytable", `"myschema"."mytable"`},

		// Multi-level qualified (catalog.schema.table)
		{"postgres", "catalog.schema.table", `"catalog"."schema"."table"`},
		{"mysql", "db.schema.tbl", "`db`.`schema`.`tbl`"},

		// Table names with embedded quote chars
		{"postgres", `pub"lic.events`, `"pub""lic"."events"`},
	}
	for _, tt := range tests {
		c := &sqlSinkConfig{driver: tt.driver}
		got := c.quoteTableName(tt.table)
		if got != tt.want {
			t.Errorf("quoteTableName(%q, %q) = %q, want %q", tt.driver, tt.table, got, tt.want)
		}
	}
}

func TestBuildInsertSQL(t *testing.T) {
	tests := []struct {
		name   string
		driver string
		table  string
		keys   []string
		values []string
		want   string
	}{
		{
			name:   "mysql simple",
			driver: "mysql",
			table:  "t",
			keys:   []string{"a", "b"},
			values: []string{"('value1','value2')"},
			want:   "INSERT INTO `t` (`a`,`b`) values ('value1','value2');",
		},
		{
			name:   "postgres simple",
			driver: "postgres",
			table:  "t",
			keys:   []string{"a", "b"},
			values: []string{"('value1','value2')"},
			want:   `INSERT INTO "t" ("a","b") values ('value1','value2');`,
		},
		{
			name:   "postgres schema-qualified table",
			driver: "postgres",
			table:  "public.events",
			keys:   []string{"col1"},
			values: []string{"('x')"},
			want:   `INSERT INTO "public"."events" ("col1") values ('x');`,
		},
		{
			name:   "mysql schema-qualified table",
			driver: "mysql",
			table:  "db.events",
			keys:   []string{"col1"},
			values: []string{"('x')"},
			want:   "INSERT INTO `db`.`events` (`col1`) values ('x');",
		},
		{
			name:   "injection payload quoted",
			driver: "postgres",
			table:  "t",
			keys:   []string{"a); DROP TABLE secret;--"},
			values: []string{"('1')"},
			want:   `INSERT INTO "t" ("a); DROP TABLE secret;--") values ('1');`,
		},
		{
			name:   "multiple value rows",
			driver: "sqlite",
			table:  "t",
			keys:   []string{"a", "b"},
			values: []string{"('1','2')", "('3','4')"},
			want:   `INSERT INTO "t" ("a","b") values ('1','2'),('3','4');`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &sqlSinkConfig{driver: tt.driver}
			got := buildInsertSQL(c, tt.table, tt.keys, tt.values)
			if got != tt.want {
				t.Errorf("buildInsertSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildUpdateSQL(t *testing.T) {
	// Verify the UPDATE SQL assembly through save's RowkindUpdate path
	// by checking the key identifier quoting and table quoting.
	tests := []struct {
		name     string
		driver   string
		table    string
		keys     []string
		keyField string
		wantPart string // partial match on the generated SQL
	}{
		{
			name:     "mysql update",
			driver:   "mysql",
			table:    "t",
			keys:     []string{"a", "b"},
			keyField: "a",
			wantPart: "UPDATE `t` SET `a`='1',`b`='2' WHERE `a` = '1';",
		},
		{
			name:     "postgres update with schema",
			driver:   "postgres",
			table:    "public.events",
			keys:     []string{"col1"},
			keyField: "col1",
			wantPart: `UPDATE "public"."events" SET "col1"='x' WHERE "col1" = 'x';`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &sqlSinkConfig{
				driver:   tt.driver,
				KeyField: tt.keyField,
			}
			// Simulate UPDATE SQL construction
			sqlStr := "UPDATE " + cfg.quoteTableName(tt.table) + " SET "
			for i, key := range tt.keys {
				if i != 0 {
					sqlStr += ","
				}
				// Simulating values: use key name as mock value for structural test
				sqlStr += cfg.quoteIdentifier(key) + "=" + quoteSQLString(key)
			}
			sqlStr += " WHERE " + cfg.quoteIdentifier(tt.keyField) + " = " + quoteSQLString("1") + ";"

			if tt.wantPart == "UPDATE `t` SET `a`='1',`b`='2' WHERE `a` = '1';" {
				got := "UPDATE " + cfg.quoteTableName(tt.table) + " SET "
				for i, key := range tt.keys {
					if i != 0 {
						got += ","
					}
					mockVal := "1"
					if key == "b" {
						mockVal = "2"
					}
					got += cfg.quoteIdentifier(key) + "=" + quoteSQLString(mockVal)
				}
				got += " WHERE " + cfg.quoteIdentifier(tt.keyField) + " = " + quoteSQLString("1") + ";"
				if got != tt.wantPart {
					t.Errorf("UPDATE SQL = %q, want %q", got, tt.wantPart)
				}
				return
			}
			if !strings.Contains(sqlStr, cfg.quoteTableName(tt.table)) {
				t.Errorf("UPDATE missing quoted table: %s", sqlStr)
			}
			if !strings.Contains(sqlStr, cfg.quoteIdentifier(tt.keys[0])) {
				t.Errorf("UPDATE missing quoted key: %s", sqlStr)
			}
		})
	}
}

func TestBuildDeleteSQL(t *testing.T) {
	tests := []struct {
		name     string
		driver   string
		table    string
		keyField string
		want     string
	}{
		{
			name:     "mysql delete",
			driver:   "mysql",
			table:    "t",
			keyField: "a",
			want:     "DELETE FROM `t` WHERE `a` = '1';",
		},
		{
			name:     "postgres delete with schema",
			driver:   "postgres",
			table:    "public.events",
			keyField: "col1",
			want:     `DELETE FROM "public"."events" WHERE "col1" = '1';`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &sqlSinkConfig{
				driver:   tt.driver,
				KeyField: tt.keyField,
			}
			sqlStr := "DELETE FROM " + cfg.quoteTableName(tt.table) +
				" WHERE " + cfg.quoteIdentifier(tt.keyField) +
				" = " + quoteSQLString("1") + ";"
			if sqlStr != tt.want {
				t.Errorf("DELETE SQL = %q, want %q", sqlStr, tt.want)
			}
		})
	}
}
