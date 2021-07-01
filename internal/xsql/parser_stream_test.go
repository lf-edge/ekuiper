package xsql

import (
	"fmt"
	"github.com/emqx/kuiper/internal/testx"
	"github.com/emqx/kuiper/pkg/ast"
	"reflect"
	"strings"
	"testing"
)

func TestParser_ParseCreateStream(t *testing.T) {
	var tests = []struct {
		s    string
		stmt *ast.StreamStmt
		err  string
	}{
		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					NICKNAMES ARRAY(STRING),
					data bytea,
					Gender BOOLEAN,
					ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID", CONF_KEY="srv1", type="MQTT", TIMESTAMP="USERID", TIMESTAMP_FORMAT="yyyy-MM-dd''T''HH:mm:ssX'");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "USERID", FieldType: &ast.BasicType{Type: ast.BIGINT}},
					{Name: "FIRST_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
					{Name: "LAST_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
					{Name: "NICKNAMES", FieldType: &ast.ArrayType{Type: ast.STRINGS}},
					{Name: "data", FieldType: &ast.BasicType{Type: ast.BYTEA}},
					{Name: "Gender", FieldType: &ast.BasicType{Type: ast.BOOLEAN}},
					{Name: "ADDRESS", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "STREET_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							{Name: "NUMBER", FieldType: &ast.BasicType{Type: ast.BIGINT}},
						},
					}},
				},
				Options: &ast.Options{
					DATASOURCE:       "users",
					FORMAT:           "JSON",
					KEY:              "USERID",
					CONF_KEY:         "srv1",
					TYPE:             "MQTT",
					TIMESTAMP:        "USERID",
					TIMESTAMP_FORMAT: "yyyy-MM-dd''T''HH:mm:ssX'",
				},
			},
		},

		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID", STRICT_VALIDATION="true", SHARED="true");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "USERID", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
				Options: &ast.Options{
					DATASOURCE:        "users",
					FORMAT:            "JSON",
					KEY:               "USERID",
					STRICT_VALIDATION: true,
					SHARED:            true,
				},
			},
		},

		{
			s: `CREATE STREAM demo (
					ADDRESSES ARRAY(STRUCT(STREET_NAME STRING, NUMBER BIGINT)),
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID", STRICT_VALIDATION="FAlse");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "ADDRESSES", FieldType: &ast.ArrayType{
						Type: ast.STRUCT,
						FieldType: &ast.RecType{
							StreamFields: []ast.StreamField{
								{Name: "STREET_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
								{Name: "NUMBER", FieldType: &ast.BasicType{Type: ast.BIGINT}},
							},
						},
					}},
				},
				Options: &ast.Options{
					DATASOURCE:        "users",
					FORMAT:            "JSON",
					KEY:               "USERID",
					STRICT_VALIDATION: false,
				},
			},
		},

		{
			s: `CREATE STREAM demo (
					ADDRESSES ARRAY(STRUCT(STREET_NAME STRING, NUMBER BIGINT)),
					birthday datetime,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "ADDRESSES", FieldType: &ast.ArrayType{
						Type: ast.STRUCT,
						FieldType: &ast.RecType{
							StreamFields: []ast.StreamField{
								{Name: "STREET_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
								{Name: "NUMBER", FieldType: &ast.BasicType{Type: ast.BIGINT}},
							},
						},
					}},
					{Name: "birthday", FieldType: &ast.BasicType{Type: ast.DATETIME}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					FORMAT:     "JSON",
					KEY:        "USERID",
				},
			},
		},

		{
			s: `CREATE STREAM demo (
					NAME string,
					ADDRESSES ARRAY(STRUCT(STREET_NAME STRING, NUMBER BIGINT)),
					birthday datetime,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
					{Name: "ADDRESSES", FieldType: &ast.ArrayType{
						Type: ast.STRUCT,
						FieldType: &ast.RecType{
							StreamFields: []ast.StreamField{
								{Name: "STREET_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
								{Name: "NUMBER", FieldType: &ast.BasicType{Type: ast.BIGINT}},
							},
						},
					}},
					{Name: "birthday", FieldType: &ast.BasicType{Type: ast.DATETIME}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					FORMAT:     "JSON",
					KEY:        "USERID",
				},
			},
		},

		{
			s: `CREATE STREAM demo (
		
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
				Options: &ast.Options{
					DATASOURCE: "users",
					FORMAT:     "JSON",
					KEY:        "USERID",
				},
			},
		},

		{
			s: `CREATE STREAM demo() WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
				Options: &ast.Options{
					DATASOURCE: "users",
					FORMAT:     "JSON",
					KEY:        "USERID",
				},
			},
		},

		{
			s: `CREATE STREAM demo (NAME string)
				 WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID", STRICT_VALIDATION="true1");`, //Invalid STRICT_VALIDATION value
			stmt: nil,
			err:  `found "true1", expect TRUE/FALSE value in STRICT_VALIDATION option.`,
		},

		{
			s: `CREATE STREAM demo (NAME string) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					FORMAT:     "JSON",
					KEY:        "USERID",
				},
			},
		},

		{
			s: `CREATE STREAM demo (NAME string)) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
				Options:      nil,
			},
			err: `found ")", expect stream options.`,
		},

		{
			s: `CREATE STREAM demo (NAME string) WITHs (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name:         ast.StreamName("demo"),
				StreamFields: nil,
				Options:      nil,
			},
			err: `found "WITHs", expected is with.`,
		},

		{
			s: `CREATE STREAM demo (NAME integer) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name:         "demo",
				StreamFields: nil,
				Options:      nil,
			},
			err: `found "integer", expect valid stream field types(BIGINT | FLOAT | STRINGS | DATETIME | BOOLEAN | BYTEA | ARRAY | STRUCT).`,
		},

		{
			s: `CREATE STREAM demo (NAME string) WITH (sources="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name:         "demo",
				StreamFields: nil,
				Options:      nil,
			},
			err: `found "sources", unknown option keys(DATASOURCE|FORMAT|KEY|CONF_KEY|SHARED|STRICT_VALIDATION|TYPE|TIMESTAMP|TIMESTAMP_FORMAT|RETAIN_SIZE).`,
		},

		{
			s: `CREATE STREAM demo ((NAME string) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name:         "demo",
				StreamFields: nil,
				Options:      nil,
			},
			err: `found "(", expect stream field name.`,
		},

		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
				) WITH ();`,
			stmt: &ast.StreamStmt{
				Name: "demo",
				StreamFields: []ast.StreamField{
					{Name: "USERID", FieldType: &ast.BasicType{Type: ast.BIGINT}},
				},
				Options: &ast.Options{},
			},
		},

		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
				) WITH ());`,
			stmt: &ast.StreamStmt{
				Name:         "",
				StreamFields: nil,
				Options:      nil,
			},
			err: `found ")", expected semicolon or EOF.`,
		},

		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
				) WITH DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &ast.StreamStmt{
				Name:         "",
				StreamFields: nil,
				Options:      nil,
			},
			//TODO The error string should be more accurate
			err: `found "DATASOURCE", expect stream options.`,
		},
		{
			s: `CREATE STREAM test(
					userID bigint,
					username string,
					NICKNAMES array(string),
					Gender boolean,
					ADDRESS struct(
						TREET_NAME string, NUMBER bigint
					), 
					INFO struct(
						INFO_NAME string, NUMBER bigint
					)
				) WITH (DATASOURCE="test", FORMAT="JSON", CONF_KEY="democonf", TYPE="MQTT");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("test"),
				StreamFields: []ast.StreamField{
					{Name: "userID", FieldType: &ast.BasicType{Type: ast.BIGINT}},
					{Name: "username", FieldType: &ast.BasicType{Type: ast.STRINGS}},
					{Name: "NICKNAMES", FieldType: &ast.ArrayType{Type: ast.STRINGS}},
					{Name: "Gender", FieldType: &ast.BasicType{Type: ast.BOOLEAN}},
					{Name: "ADDRESS", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "TREET_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							{Name: "NUMBER", FieldType: &ast.BasicType{Type: ast.BIGINT}},
						},
					}},
					{Name: "INFO", FieldType: &ast.RecType{
						StreamFields: []ast.StreamField{
							{Name: "INFO_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
							{Name: "NUMBER", FieldType: &ast.BasicType{Type: ast.BIGINT}},
						},
					}},
				},
				Options: &ast.Options{
					DATASOURCE: "test",
					FORMAT:     "JSON",
					CONF_KEY:   "democonf",
					TYPE:       "MQTT",
				},
			},
		}, {
			s: `CREATE STREAM demo (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					PICTURE BYTEA,
				) WITH (DATASOURCE="users", FORMAT="JSON");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "USERID", FieldType: &ast.BasicType{Type: ast.BIGINT}},
					{Name: "FIRST_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
					{Name: "LAST_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
					{Name: "PICTURE", FieldType: &ast.BasicType{Type: ast.BYTEA}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					FORMAT:     "JSON",
				},
			},
		}, {
			s: `CREATE STREAM demo (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					PICTURE BYTEA,
				) WITH (DATASOURCE="users", FORMAT="JSON");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "USERID", FieldType: &ast.BasicType{Type: ast.BIGINT}},
					{Name: "FIRST_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
					{Name: "LAST_NAME", FieldType: &ast.BasicType{Type: ast.STRINGS}},
					{Name: "PICTURE", FieldType: &ast.BasicType{Type: ast.BYTEA}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					FORMAT:     "JSON",
				},
			},
		}, {
			s: `CREATE STREAM demo (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					PICTURE BYTEA,
				) WITH (DATASOURCE="users", format="BINARY");`,
			stmt: &ast.StreamStmt{
				Name:         "",
				StreamFields: nil,
				Options:      nil,
			},
			err: "'binary' format stream can have only one field",
		}, {
			s: `CREATE STREAM demo (
					image BYTEA
				) WITH (DATASOURCE="users", FORMAT="BINARY");`,
			stmt: &ast.StreamStmt{
				Name: ast.StreamName("demo"),
				StreamFields: []ast.StreamField{
					{Name: "image", FieldType: &ast.BasicType{Type: ast.BYTEA}},
				},
				Options: &ast.Options{
					DATASOURCE: "users",
					FORMAT:     "BINARY",
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, err := NewParser(strings.NewReader(tt.s)).ParseCreateStmt()
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}

}
