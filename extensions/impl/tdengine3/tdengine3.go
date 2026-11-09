// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package tdengine3

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	_ "github.com/taosdata/driver-go/v3/taosWS"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type TaosConfig struct {
	ProvideTs    bool     `json:"provideTs"`
	Port         int      `json:"port"`
	Host         string   `json:"host"`
	User         string   `json:"user"`
	Password     string   `json:"password"`
	Database     string   `json:"database"`
	Table        string   `json:"table"`
	TsFieldName  string   `json:"tsFieldName"`
	Fields       []string `json:"fields"`
	STable       string   `json:"sTable"`
	TagFields    []string `json:"tagFields"`
	DataTemplate string   `json:"dataTemplate"`
	DataField    string   `json:"dataField"`
}

type tdengineSink3 struct {
	cfg *TaosConfig
	cli *sql.DB
}

func (t *tdengineSink3) Ping(ctx api.StreamContext, props map[string]any) error {
	url := fmt.Sprintf(`%s:%s@tcp(%s)/%s`, t.cfg.User, t.cfg.Password, cast.JoinHostPortInt(t.cfg.Host, t.cfg.Port), t.cfg.Database)
	taos, err := sql.Open("taosSql", url)
	if err != nil {
		return fmt.Errorf("Failed to connect to tdengine3: %s", err)
	}

	defer taos.Close()
	return nil
}

func (t *tdengineSink3) Provision(ctx api.StreamContext, props map[string]any) error {
	t.cfg = &TaosConfig{
		Host:     "localhost",
		Port:     6041,
		User:     "root",
		Password: "taosdata",
	}
	err := cast.MapToStruct(props, t.cfg)
	if err != nil {
		return err
	}
	if t.cfg.Database == "" {
		return fmt.Errorf("property database is required")
	}
	if t.cfg.Table == "" {
		return fmt.Errorf("property table is required")
	}
	if t.cfg.TsFieldName == "" {
		return fmt.Errorf("property TsFieldName is required")
	}
	if t.cfg.STable != "" && len(t.cfg.TagFields) == 0 {
		return fmt.Errorf("property tagFields is required when sTable is set")
	}
	return nil
}

func (t *tdengineSink3) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("tdengine3 sink connection")
	url := fmt.Sprintf(`%s:%s@ws(%s)/%s`, t.cfg.User, t.cfg.Password, cast.JoinHostPortInt(t.cfg.Host, t.cfg.Port), t.cfg.Database)
	taosCli, err := sql.Open("taosWS", url)
	t.cli = taosCli
	return err
}

func (t *tdengineSink3) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("tdengine3 sink close")
	t.cli.Close()
	return nil
}

func (t *tdengineSink3) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	sqlStr, sqlE := t.cfg.buildSql(item)
	if sqlE != nil {
		return fmt.Errorf("failed to build sql to tdengine3: %s", sqlE)
	}
	ctx.GetLogger().Debugf("tdengine3 sink collect sql: %s", sqlStr)
	_, e := t.cli.Exec(sqlStr)
	if e != nil {
		return fmt.Errorf("failed to exec sql to tdengine3: %s", e)
	}
	return nil
}

func (t *tdengineSink3) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	items.RangeOfTuples(func(_ int, tuple api.MessageTuple) bool {
		err := t.Collect(ctx, tuple)
		if err != nil {
			ctx.GetLogger().Error(err)
		}
		return true
	})
	return nil
}

func (cfg *TaosConfig) buildSql(item api.MessageTuple) (string, error) {
	mapData := item.ToMap()
	var keys, vals, tags []string
	if len(mapData) == 0 {
		return "", fmt.Errorf("data is empty")
	}
	table := cfg.Table
	if dp, ok := item.(api.HasDynamicProps); ok {
		temp, transformed := dp.DynamicProps(table)
		if transformed {
			if !isDataIdentifier(temp, true) {
				return "", fmt.Errorf("invalid dynamic table name: %q", temp)
			}
			table = temp
		}
	}

	sTable := cfg.STable
	if dp, ok := item.(api.HasDynamicProps); ok {
		temp, transformed := dp.DynamicProps(sTable)
		if transformed {
			if !isDataIdentifier(temp, true) {
				return "", fmt.Errorf("invalid dynamic super table name: %q", temp)
			}
			sTable = temp
		}
	}

	if cfg.ProvideTs {
		if v, ok := mapData[cfg.TsFieldName]; !ok {
			return "", fmt.Errorf("timestamp field not found : %s", cfg.TsFieldName)
		} else {
			value, err := sqlTimestamp(v)
			if err != nil {
				return "", fmt.Errorf("timestamp field %q: %w", cfg.TsFieldName, err)
			}
			keys = append(keys, cfg.TsFieldName)
			vals = append(vals, value)
		}
	} else {
		vals = append(vals, "now")
		keys = append(keys, cfg.TsFieldName)
	}

	for _, k := range cfg.TagFields {
		value, err := sqlValue(mapData[k])
		if err != nil {
			return "", fmt.Errorf("tag field %q: %w", k, err)
		}
		tags = append(tags, value)
	}

	if len(cfg.Fields) != 0 {
		for _, k := range cfg.Fields {
			if k == cfg.TsFieldName {
				continue
			}
			if contains(cfg.TagFields, k) {
				continue
			}
			if v, ok := mapData[k]; ok {
				value, err := sqlValue(v)
				if err != nil {
					return "", fmt.Errorf("field %q: %w", k, err)
				}
				keys = append(keys, k)
				vals = append(vals, value)
			} else {
				return "", fmt.Errorf("field not found : %s", k)
			}
		}
	} else {
		for k, v := range mapData {
			if k == cfg.TsFieldName {
				continue
			}
			if contains(cfg.TagFields, k) {
				continue
			}
			if !isDataIdentifier(k, false) {
				return "", fmt.Errorf("invalid data field name: %q", k)
			}
			value, err := sqlValue(v)
			if err != nil {
				return "", fmt.Errorf("field %q: %w", k, err)
			}
			keys = append(keys, k)
			vals = append(vals, value)
		}
	}

	sqlStr := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(keys, ","))
	if sTable != "" {
		sqlStr += " USING " + sTable
	}
	if len(tags) != 0 {
		sqlStr += " TAGS(" + strings.Join(tags, ",") + ")"
	}
	sqlStr += " values (" + strings.Join(vals, ",") + ")"
	return sqlStr, nil
}

// TDengine string literals use backslash escapes. Escape the backslash itself
// as well as the delimiter so input cannot close the surrounding literal.
var sqlStringEscaper = strings.NewReplacer(
	`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`, "\t", `\t`,
)

func sqlValue(v any) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	if n, ok := v.(json.Number); ok {
		f, err := n.Float64()
		if err != nil || !json.Valid([]byte(n)) || math.IsNaN(f) || math.IsInf(f, 0) {
			return "", fmt.Errorf("invalid number: %q", n)
		}
		return string(n), nil
	}
	// Use the underlying primitive, never a Stringer implementation supplied by
	// an input type. Unsupported values must not become raw SQL through fmt.
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		s := rv.String()
		if strings.IndexByte(s, 0) >= 0 {
			return "", fmt.Errorf("string contains NUL")
		}
		return `"` + sqlStringEscaper.Replace(s) + `"`, nil
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "", fmt.Errorf("non-finite number")
		}
		return strconv.FormatFloat(f, 'g', -1, rv.Type().Bits()), nil
	default:
		return "", fmt.Errorf("unsupported value type %T", v)
	}
}

func sqlTimestamp(v any) (string, error) {
	if v == nil {
		return "", fmt.Errorf("timestamp is null")
	}
	if n, ok := v.(json.Number); ok {
		v = string(n)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		s := rv.String()
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return strconv.FormatInt(n, 10), nil
		}
		// Older rules may supply an already quoted date. Validate its contents
		// and encode it again instead of treating the supplied quotes as SQL.
		if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') && s[len(s)-1] == s[0] {
			s = s[1 : len(s)-1]
		}
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999"} {
			if _, err := time.Parse(layout, s); err == nil {
				return sqlValue(s)
			}
		}
		return "", fmt.Errorf("invalid timestamp: %q", s)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return sqlValue(v)
	default:
		return "", fmt.Errorf("unsupported timestamp type %T", v)
	}
}

// Only stream-derived identifiers are checked. Static rule configuration is
// trusted. Preserve TDengine's unquoted case folding and backtick quoting.
func isDataIdentifier(name string, qualified bool) bool {
	parts := strings.Split(name, ".")
	if len(parts) > 2 || (!qualified && len(parts) != 1) {
		return false
	}
	for _, part := range parts {
		if len(part) == 0 {
			return false
		}
		if len(part) >= 3 && part[0] == '`' && part[len(part)-1] == '`' {
			for _, c := range part[1 : len(part)-1] {
				if c == '`' || c == '\\' || c < ' ' || c == 127 {
					return false
				}
			}
			continue
		}
		for i, c := range []byte(part) {
			if c != '_' && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(i > 0 && c >= '0' && c <= '9') {
				return false
			}
		}
	}
	return true
}

func contains(slice []string, target string) bool {
	for _, element := range slice {
		if element == target {
			return true
		}
	}
	return false
}

func GetSink() api.Sink {
	return &tdengineSink3{}
}
