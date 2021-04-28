package xsql

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"reflect"
	"strings"
	"testing"
)

func TestParser_ParseCreateStream(t *testing.T) {
	var tests = []struct {
		s    string
		stmt *StreamStmt
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
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "USERID", FieldType: &BasicType{Type: BIGINT}},
					{Name: "FIRST_NAME", FieldType: &BasicType{Type: STRINGS}},
					{Name: "LAST_NAME", FieldType: &BasicType{Type: STRINGS}},
					{Name: "NICKNAMES", FieldType: &ArrayType{Type: STRINGS}},
					{Name: "data", FieldType: &BasicType{Type: BYTEA}},
					{Name: "Gender", FieldType: &BasicType{Type: BOOLEAN}},
					{Name: "ADDRESS", FieldType: &RecType{
						StreamFields: []StreamField{
							{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
							{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
						},
					}},
				},
				Options: map[string]string{
					"DATASOURCE":       "users",
					"FORMAT":           "JSON",
					"KEY":              "USERID",
					"CONF_KEY":         "srv1",
					"TYPE":             "MQTT",
					"TIMESTAMP":        "USERID",
					"TIMESTAMP_FORMAT": "yyyy-MM-dd''T''HH:mm:ssX'",
				},
			},
		},

		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID", STRICT_VALIDATION="true");`,
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "USERID", FieldType: &BasicType{Type: BIGINT}},
				},
				Options: map[string]string{
					"DATASOURCE":        "users",
					"FORMAT":            "JSON",
					"KEY":               "USERID",
					"STRICT_VALIDATION": "true",
				},
			},
		},

		{
			s: `CREATE STREAM demo (
					ADDRESSES ARRAY(STRUCT(STREET_NAME STRING, NUMBER BIGINT)),
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID", STRICT_VALIDATION="FAlse");`,
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "ADDRESSES", FieldType: &ArrayType{
						Type: STRUCT,
						FieldType: &RecType{
							StreamFields: []StreamField{
								{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
								{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
							},
						},
					}},
				},
				Options: map[string]string{
					"DATASOURCE":        "users",
					"FORMAT":            "JSON",
					"KEY":               "USERID",
					"STRICT_VALIDATION": "FAlse",
				},
			},
		},

		{
			s: `CREATE STREAM demo (
					ADDRESSES ARRAY(STRUCT(STREET_NAME STRING, NUMBER BIGINT)),
					birthday datetime,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "ADDRESSES", FieldType: &ArrayType{
						Type: STRUCT,
						FieldType: &RecType{
							StreamFields: []StreamField{
								{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
								{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
							},
						},
					}},
					{Name: "birthday", FieldType: &BasicType{Type: DATETIME}},
				},
				Options: map[string]string{
					"DATASOURCE": "users",
					"FORMAT":     "JSON",
					"KEY":        "USERID",
				},
			},
		},

		{
			s: `CREATE STREAM demo (
					NAME string,
					ADDRESSES ARRAY(STRUCT(STREET_NAME STRING, NUMBER BIGINT)),
					birthday datetime,
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "NAME", FieldType: &BasicType{Type: STRINGS}},
					{Name: "ADDRESSES", FieldType: &ArrayType{
						Type: STRUCT,
						FieldType: &RecType{
							StreamFields: []StreamField{
								{Name: "STREET_NAME", FieldType: &BasicType{Type: STRINGS}},
								{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
							},
						},
					}},
					{Name: "birthday", FieldType: &BasicType{Type: DATETIME}},
				},
				Options: map[string]string{
					"DATASOURCE": "users",
					"FORMAT":     "JSON",
					"KEY":        "USERID",
				},
			},
		},

		{
			s: `CREATE STREAM demo (
		
				) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name:         StreamName("demo"),
				StreamFields: nil,
				Options: map[string]string{
					"DATASOURCE": "users",
					"FORMAT":     "JSON",
					"KEY":        "USERID",
				},
			},
		},

		{
			s: `CREATE STREAM demo() WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name:         StreamName("demo"),
				StreamFields: nil,
				Options: map[string]string{
					"DATASOURCE": "users",
					"FORMAT":     "JSON",
					"KEY":        "USERID",
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
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "NAME", FieldType: &BasicType{Type: STRINGS}},
				},
				Options: map[string]string{
					"DATASOURCE": "users",
					"FORMAT":     "JSON",
					"KEY":        "USERID",
				},
			},
		},

		{
			s: `CREATE STREAM demo (NAME string)) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name:         StreamName("demo"),
				StreamFields: nil,
				Options:      nil,
			},
			err: `found ")", expect stream options.`,
		},

		{
			s: `CREATE STREAM demo (NAME string) WITHs (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name:         StreamName("demo"),
				StreamFields: nil,
				Options:      nil,
			},
			err: `found "WITHs", expected is with.`,
		},

		{
			s: `CREATE STREAM demo (NAME integer) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name:         "demo",
				StreamFields: nil,
				Options:      nil,
			},
			err: `found "integer", expect valid stream field types(BIGINT | FLOAT | STRINGS | DATETIME | BOOLEAN | BYTEA | ARRAY | STRUCT).`,
		},

		{
			s: `CREATE STREAM demo (NAME string) WITH (sources="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
				Name:         "demo",
				StreamFields: nil,
				Options:      nil,
			},
			err: `found "sources", unknown option keys(DATASOURCE|FORMAT|KEY|CONF_KEY|STRICT_VALIDATION|TYPE).`,
		},

		{
			s: `CREATE STREAM demo ((NAME string) WITH (DATASOURCE="users", FORMAT="JSON", KEY="USERID");`,
			stmt: &StreamStmt{
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
			stmt: &StreamStmt{
				Name: "demo",
				StreamFields: []StreamField{
					{Name: "USERID", FieldType: &BasicType{Type: BIGINT}},
				},
				Options: map[string]string{},
			},
		},

		{
			s: `CREATE STREAM demo (
					USERID BIGINT,
				) WITH ());`,
			stmt: &StreamStmt{
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
			stmt: &StreamStmt{
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
			stmt: &StreamStmt{
				Name: StreamName("test"),
				StreamFields: []StreamField{
					{Name: "userID", FieldType: &BasicType{Type: BIGINT}},
					{Name: "username", FieldType: &BasicType{Type: STRINGS}},
					{Name: "NICKNAMES", FieldType: &ArrayType{Type: STRINGS}},
					{Name: "Gender", FieldType: &BasicType{Type: BOOLEAN}},
					{Name: "ADDRESS", FieldType: &RecType{
						StreamFields: []StreamField{
							{Name: "TREET_NAME", FieldType: &BasicType{Type: STRINGS}},
							{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
						},
					}},
					{Name: "INFO", FieldType: &RecType{
						StreamFields: []StreamField{
							{Name: "INFO_NAME", FieldType: &BasicType{Type: STRINGS}},
							{Name: "NUMBER", FieldType: &BasicType{Type: BIGINT}},
						},
					}},
				},
				Options: map[string]string{
					"DATASOURCE": "test",
					"FORMAT":     "JSON",
					"CONF_KEY":   "democonf",
					"TYPE":       "MQTT",
				},
			},
		}, {
			s: `CREATE STREAM demo (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					PICTURE BYTEA,
				) WITH (DATASOURCE="users", FORMAT="JSON");`,
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "USERID", FieldType: &BasicType{Type: BIGINT}},
					{Name: "FIRST_NAME", FieldType: &BasicType{Type: STRINGS}},
					{Name: "LAST_NAME", FieldType: &BasicType{Type: STRINGS}},
					{Name: "PICTURE", FieldType: &BasicType{Type: BYTEA}},
				},
				Options: map[string]string{
					"DATASOURCE": "users",
					"FORMAT":     "JSON",
				},
			},
		}, {
			s: `CREATE STREAM demo (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					PICTURE BYTEA,
				) WITH (DATASOURCE="users", FORMAT="JSON");`,
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "USERID", FieldType: &BasicType{Type: BIGINT}},
					{Name: "FIRST_NAME", FieldType: &BasicType{Type: STRINGS}},
					{Name: "LAST_NAME", FieldType: &BasicType{Type: STRINGS}},
					{Name: "PICTURE", FieldType: &BasicType{Type: BYTEA}},
				},
				Options: map[string]string{
					"DATASOURCE": "users",
					"FORMAT":     "JSON",
				},
			},
		}, {
			s: `CREATE STREAM demo (
					USERID BIGINT,
					FIRST_NAME STRING,
					LAST_NAME STRING,
					PICTURE BYTEA,
				) WITH (DATASOURCE="users", format="BINARY");`,
			stmt: &StreamStmt{
				Name:         "",
				StreamFields: nil,
				Options:      nil,
			},
			err: "'binary' format stream can have only one field",
		}, {
			s: `CREATE STREAM demo (
					image BYTEA
				) WITH (DATASOURCE="users", FORMAT="BINARY");`,
			stmt: &StreamStmt{
				Name: StreamName("demo"),
				StreamFields: []StreamField{
					{Name: "image", FieldType: &BasicType{Type: BYTEA}},
				},
				Options: map[string]string{
					"DATASOURCE": "users",
					"FORMAT":     "BINARY",
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, err := NewParser(strings.NewReader(tt.s)).ParseCreateStmt()
		if !reflect.DeepEqual(tt.err, common.Errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}

}
