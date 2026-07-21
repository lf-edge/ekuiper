// Copyright 2025-2026 EMQ Technologies Co., Ltd.
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
	"testing"
)

func TestSplitDblink(t *testing.T) {
	tests := []struct {
		input      string
		wantID     string
		wantDblink string
	}{
		{"events", "events", ""},
		{"events@remote", "events", "@remote"},
		{"events@remote.db", "events", "@remote.db"},
		{`"MixedCase"@remote`, `"MixedCase"`, "@remote"},
		{"public.events@remote", "public.events", "@remote"},
		{"a@b.c@d", "a", "@b.c@d"},
		{`"a@b".c`, `"a@b".c`, ""},
		{`"a"@b.c`, `"a"`, "@b.c"},
	}
	for _, tt := range tests {
		idPart, dblink := splitDblink(tt.input, '"')
		if idPart != tt.wantID || dblink != tt.wantDblink {
			t.Errorf("splitDblink(%q) = (%q, %q), want (%q, %q)",
				tt.input, idPart, dblink, tt.wantID, tt.wantDblink)
		}
	}
}

func TestIdentifierQuoteChar(t *testing.T) {
	tests := []struct {
		driver string
		want   string
	}{
		// MySQL family — backtick
		{"mysql", "`"},
		{"MYSQL", "`"},
		{"MySQL", "`"},
		{"mymysql", "`"},
		{"MyMySQL", "`"},
		{"hive", "`"},
		{"spanner", "`"},
		{"impala", "`"},

		// SQL standard — double quote
		{"postgres", "\""},
		{"PostgreSQL", "\""},
		{"pgx", "\""},
		{"sqlite", "\""},
		{"sqlserver", "\""},
		{"mssql", "\""},
		{"oracle", "\""},
		{"godror", "\""},
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

func TestNormalizeIdentifier(t *testing.T) {
	tests := []struct {
		driver string
		name   string
		want   string
	}{
		// Oracle family — uppercase
		{"oracle", "events", "EVENTS"},
		{"oracle", "MY_TABLE", "MY_TABLE"},
		{"oracle", "MixedCase", "MIXEDCASE"},
		{"godror", "events", "EVENTS"},

		// PostgreSQL family — lowercase
		{"postgres", "Events", "events"},
		{"postgres", "MY_TABLE", "my_table"},
		{"postgres", "mixedCase", "mixedcase"},
		{"pgx", "Events", "events"},

		// Others — preserve
		{"mysql", "Events", "Events"},
		{"mymysql", "Events", "Events"},
		{"sqlite", "Events", "Events"},
		{"sqlserver", "Events", "Events"},
		{"hive", "Events", "Events"},
		{"spanner", "Events", "Events"},
		{"unknown", "Events", "Events"},
	}
	for _, tt := range tests {
		c := &sqlSinkConfig{driver: tt.driver}
		got := c.normalizeIdentifier(tt.name)
		if got != tt.want {
			t.Errorf("normalizeIdentifier(%q, %q) = %q, want %q", tt.driver, tt.name, got, tt.want)
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

		// Extended MySQL-family drivers
		{"mymysql", "a", "`a`"},
		{"hive", "a", "`a`"},
		{"spanner", "t", "`t`"},
		{"impala", "t", "`t`"},

		// PostgreSQL/SQLite double-quote quoting with case normalization
		{"postgres", "a", `"a"`},
		{"sqlite", "my_column", `"my_column"`},
		{"postgres", `a"b`, `"a""b"`},      // embedded double-quote doubled
		{"postgres", "Events", `"events"`}, // folded to lowercase per PG unquoted rules
		{"postgres", "MY_TABLE", `"my_table"`},

		// Oracle — uppercase normalization
		{"oracle", "events", `"EVENTS"`},
		{"oracle", "MixedCase", `"MIXEDCASE"`},
		{"godror", "events", `"EVENTS"`},

		// Already-quoted identifiers — preserved as-is, no case normalization.
		// This respects the operator's explicit casing choice (e.g. Oracle "MixedCase").
		{"oracle", `"MixedCase"`, `"MixedCase"`},
		{"postgres", `"MixedCase"`, `"MixedCase"`},
		{"mysql", "`MixedCase`", "`MixedCase`"},
		{"postgres", `"x") VALUES ('safe'); DROP TABLE secret;--"`, `"""x"") values ('safe'); drop table secret;--"""`},

		// SQL injection payloads — metacharacters become part of quoted identifier;
		// PostgreSQL lowercases per its unquoted-identifier rules, but the injection
		// is still neutralized because the entire payload is inside a quoted identifier.
		{"postgres", "a); DROP TABLE secret;--", `"a); drop table secret;--"`},
		{"mysql", "a); DROP TABLE secret;--", "`a); DROP TABLE secret;--`"},
		{"postgres", "a) values ('1'); CREATE TABLE pwned(z); DROP TABLE secret; --", `"a) values ('1'); create table pwned(z); drop table secret; --"`},
		{"mysql", "a) values ('1'); CREATE TABLE pwned(z); DROP TABLE secret; --", "`a) values ('1'); CREATE TABLE pwned(z); DROP TABLE secret; --`"},

		// Embedded quote with injection combined
		{"postgres", `a"; DROP TABLE t;--`, `"a""; drop table t;--"`},
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

		// Extended MySQL-family
		{"mymysql", "t", "`t`"},
		{"hive", "t", "`t`"},
		{"spanner", "t", "`t`"},
		{"impala", "t", "`t`"},

		// Schema-qualified table names
		{"postgres", "public.events", `"public"."events"`},
		{"mysql", "mydb.mytable", "`mydb`.`mytable`"},

		// Schema-qualified with case normalization (PostgreSQL)
		{"postgres", "Public.Events", `"public"."events"`},
		{"postgres", "MYSCHEMA.MYTABLE", `"myschema"."mytable"`},

		// Oracle schema-qualified — uppercased
		{"oracle", "myschema.mytable", `"MYSCHEMA"."MYTABLE"`},

		// Oracle dblink syntax — @dblink preserved unquoted after quoted identifier
		{"oracle", "events@remote", `"EVENTS"@remote`},
		{"oracle", "myschema.mytable@remote", `"MYSCHEMA"."MYTABLE"@remote`},

		// Already-quoted table names — preserved as-is
		{"oracle", `"MixedCase"`, `"MixedCase"`},
		{"postgres", `"MyTable"`, `"MyTable"`},
		{"mysql", "`MyTable`", "`MyTable`"},
		{"postgres", `"audit.v1"`, `"audit.v1"`},
		{"postgres", `"My.Schema"."Events"`, `"My.Schema"."Events"`},
		{"mysql", "`a@b`", "`a@b`"},

		// Already-quoted with dblink
		{"oracle", `"MixedCase"@remote`, `"MixedCase"@remote`},

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
			name:   "postgres mixed-case normalized",
			driver: "postgres",
			table:  "MyTable",
			keys:   []string{"ColA", "ColB"},
			values: []string{"('x','y')"},
			want:   `INSERT INTO "mytable" ("cola","colb") values ('x','y');`,
		},
		{
			name:   "oracle normalized to uppercase",
			driver: "oracle",
			table:  "my_table",
			keys:   []string{"col_a", "col_b"},
			values: []string{"('x','y')"},
			want:   `INSERT INTO "MY_TABLE" ("COL_A","COL_B") values ('x','y');`,
		},
		{
			name:   "oracle already-quoted table preserved",
			driver: "oracle",
			table:  `"MixedCase"`,
			keys:   []string{"a"},
			values: []string{"('x')"},
			want:   `INSERT INTO "MixedCase" ("A") values ('x');`,
		},
		{
			name:   "oracle dblink table",
			driver: "oracle",
			table:  "events@remote",
			keys:   []string{"a"},
			values: []string{"('x')"},
			want:   `INSERT INTO "EVENTS"@remote ("A") values ('x');`,
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
			name:   "mymysql backtick quoting",
			driver: "mymysql",
			table:  "t",
			keys:   []string{"a", "b"},
			values: []string{"('1','2')"},
			want:   "INSERT INTO `t` (`a`,`b`) values ('1','2');",
		},
		{
			name:   "injection payload quoted",
			driver: "postgres",
			table:  "t",
			keys:   []string{"a); DROP TABLE secret;--"},
			values: []string{"('1')"},
			want:   `INSERT INTO "t" ("a); drop table secret;--") values ('1');`,
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
	tests := []struct {
		name     string
		driver   string
		table    string
		keys     []string
		vals     []string
		keyField string
		keyval   any
		want     string
	}{
		{
			name:     "mysql update",
			driver:   "mysql",
			table:    "t",
			keys:     []string{"a", "b"},
			vals:     []string{"'1'", "'2'"},
			keyField: "a",
			keyval:   "1",
			want:     "UPDATE `t` SET `a`='1',`b`='2' WHERE `a` = '1';",
		},
		{
			name:     "postgres update with schema",
			driver:   "postgres",
			table:    "public.events",
			keys:     []string{"col1"},
			vals:     []string{"'x'"},
			keyField: "col1",
			keyval:   "1",
			want:     `UPDATE "public"."events" SET "col1"='x' WHERE "col1" = '1';`,
		},
		{
			name:     "postgres mixed-case normalization",
			driver:   "postgres",
			table:    "MyTable",
			keys:     []string{"ColA", "ColB"},
			vals:     []string{"'a'", "'b'"},
			keyField: "ColA",
			keyval:   7,
			want:     `UPDATE "mytable" SET "cola"='a',"colb"='b' WHERE "cola" = 7;`,
		},
		{
			name:     "oracle uppercase normalization",
			driver:   "oracle",
			table:    "my_table",
			keys:     []string{"col_a"},
			vals:     []string{"'x'"},
			keyField: "col_a",
			keyval:   "1",
			want:     `UPDATE "MY_TABLE" SET "COL_A"='x' WHERE "COL_A" = '1';`,
		},
		{
			name:     "oracle dblink update",
			driver:   "oracle",
			table:    "events@remote",
			keys:     []string{"col_a"},
			vals:     []string{"'x'"},
			keyField: "col_a",
			keyval:   "1",
			want:     `UPDATE "EVENTS"@remote SET "COL_A"='x' WHERE "COL_A" = '1';`,
		},
		{
			name:     "oracle already-quoted update",
			driver:   "oracle",
			table:    `"MixedCase"`,
			keys:     []string{"col_a"},
			vals:     []string{"'x'"},
			keyField: "col_a",
			keyval:   "1",
			want:     `UPDATE "MixedCase" SET "COL_A"='x' WHERE "COL_A" = '1';`,
		},
		{
			name:     "mymysql backtick",
			driver:   "mymysql",
			table:    "t",
			keys:     []string{"a"},
			keyField: "a",
			vals:     []string{"'1'"},
			keyval:   "1",
			want:     "UPDATE `t` SET `a`='1' WHERE `a` = '1';",
		},
		{
			name:     "postgres malicious identifier",
			driver:   "postgres",
			table:    "t",
			keys:     []string{`"x") VALUES ('safe'); DROP TABLE secret;--"`},
			vals:     []string{"'ignored'"},
			keyField: `"x") VALUES ('safe'); DROP TABLE secret;--"`,
			keyval:   "1",
			want:     `UPDATE "t" SET """x"") values ('safe'); drop table secret;--"""='ignored' WHERE """x"") values ('safe'); drop table secret;--""" = '1';`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &sqlSinkConfig{
				driver:   tt.driver,
				KeyField: tt.keyField,
			}
			got := buildUpdateSQL(cfg, tt.table, tt.keys, tt.vals, tt.keyField, tt.keyval)
			if got != tt.want {
				t.Errorf("buildUpdateSQL() = %q, want %q", got, tt.want)
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
		keyval   any
		want     string
	}{
		{
			name:     "mysql delete",
			driver:   "mysql",
			table:    "t",
			keyField: "a",
			keyval:   "1",
			want:     "DELETE FROM `t` WHERE `a` = '1';",
		},
		{
			name:     "postgres delete with schema",
			driver:   "postgres",
			table:    "public.events",
			keyField: "col1",
			keyval:   "1",
			want:     `DELETE FROM "public"."events" WHERE "col1" = '1';`,
		},
		{
			name:     "postgres mixed-case normalization",
			driver:   "postgres",
			table:    "MyTable",
			keyField: "RowID",
			keyval:   10,
			want:     `DELETE FROM "mytable" WHERE "rowid" = 10;`,
		},
		{
			name:     "oracle uppercase normalization",
			driver:   "oracle",
			table:    "my_table",
			keyField: "row_id",
			keyval:   "1",
			want:     `DELETE FROM "MY_TABLE" WHERE "ROW_ID" = '1';`,
		},
		{
			name:     "oracle dblink delete",
			driver:   "oracle",
			table:    "events@remote",
			keyField: "row_id",
			keyval:   "1",
			want:     `DELETE FROM "EVENTS"@remote WHERE "ROW_ID" = '1';`,
		},
		{
			name:     "oracle already-quoted delete",
			driver:   "oracle",
			table:    `"MixedCase"`,
			keyField: "row_id",
			keyval:   "1",
			want:     `DELETE FROM "MixedCase" WHERE "ROW_ID" = '1';`,
		},
		{
			name:     "mymysql backtick",
			driver:   "mymysql",
			table:    "t",
			keyField: "a",
			keyval:   "1",
			want:     "DELETE FROM `t` WHERE `a` = '1';",
		},
		{
			name:     "postgres malicious key field",
			driver:   "postgres",
			table:    "t",
			keyField: `"x") VALUES ('safe'); DROP TABLE secret;--"`,
			keyval:   "1",
			want:     `DELETE FROM "t" WHERE """x"") values ('safe'); drop table secret;--""" = '1';`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &sqlSinkConfig{
				driver:   tt.driver,
				KeyField: tt.keyField,
			}
			got := buildDeleteSQL(cfg, tt.table, tt.keyField, tt.keyval)
			if got != tt.want {
				t.Errorf("buildDeleteSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}
