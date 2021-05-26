package xsql

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/kv"
	"strings"
)

func PrintFieldType(ft FieldType) (result string) {
	switch t := ft.(type) {
	case *BasicType:
		result = t.Type.String()
	case *ArrayType:
		result = "array("
		if t.FieldType != nil {
			result += PrintFieldType(t.FieldType)
		} else {
			result += t.Type.String()
		}
		result += ")"
	case *RecType:
		result = "struct("
		isFirst := true
		for _, f := range t.StreamFields {
			if isFirst {
				isFirst = false
			} else {
				result += ", "
			}
			result = result + f.Name + " " + PrintFieldType(f.FieldType)
		}
		result += ")"
	}
	return
}

func PrintFieldTypeForJson(ft FieldType) (result interface{}) {
	r, q := doPrintFieldTypeForJson(ft)
	if q {
		return r
	} else {
		return json.RawMessage(r)
	}
}

func doPrintFieldTypeForJson(ft FieldType) (result string, isLiteral bool) {
	switch t := ft.(type) {
	case *BasicType:
		return t.Type.String(), true
	case *ArrayType:
		var (
			fieldType string
			q         bool
		)
		if t.FieldType != nil {
			fieldType, q = doPrintFieldTypeForJson(t.FieldType)
		} else {
			fieldType, q = t.Type.String(), true
		}
		if q {
			result = fmt.Sprintf(`{"Type":"array","ElementType":"%s"}`, fieldType)
		} else {
			result = fmt.Sprintf(`{"Type":"array","ElementType":%s}`, fieldType)
		}

	case *RecType:
		result = `{"Type":"struct","Fields":[`
		isFirst := true
		for _, f := range t.StreamFields {
			if isFirst {
				isFirst = false
			} else {
				result += ","
			}
			fieldType, q := doPrintFieldTypeForJson(f.FieldType)
			if q {
				result = fmt.Sprintf(`%s{"FieldType":"%s","Name":"%s"}`, result, fieldType, f.Name)
			} else {
				result = fmt.Sprintf(`%s{"FieldType":%s,"Name":"%s"}`, result, fieldType, f.Name)
			}
		}
		result += `]}`
	}
	return result, false
}

func GetStreams(stmt *SelectStatement) (result []string) {
	if stmt == nil {
		return nil
	}
	for _, source := range stmt.Sources {
		if s, ok := source.(*Table); ok {
			result = append(result, s.Name)
		}
	}

	for _, join := range stmt.Joins {
		result = append(result, join.Name)
	}
	return
}

func LowercaseKeyMap(m map[string]interface{}) map[string]interface{} {
	m1 := make(map[string]interface{})
	for k, v := range m {
		if m2, ok := v.(map[string]interface{}); ok {
			m1[strings.ToLower(k)] = LowercaseKeyMap(m2)
		} else {
			m1[strings.ToLower(k)] = v
		}
	}
	return m1
}

func GetStatementFromSql(sql string) (*SelectStatement, error) {
	parser := NewParser(strings.NewReader(sql))
	if stmt, err := Language.Parse(parser); err != nil {
		return nil, fmt.Errorf("Parse SQL %s error: %s.", sql, err)
	} else {
		if r, ok := stmt.(*SelectStatement); !ok {
			return nil, fmt.Errorf("SQL %s is not a select statement.", sql)
		} else {
			return r, nil
		}
	}
}

type StreamInfo struct {
	StreamType StreamType `json:"streamType"`
	Statement  string     `json:"statement"`
}

func GetDataSourceStatement(m kv.KeyValue, name string) (*StreamInfo, error) {
	var (
		v  string
		vs = &StreamInfo{}
	)
	err := m.Open()
	if err != nil {
		return nil, fmt.Errorf("error when opening db: %v", err)
	}
	defer m.Close()
	if ok, _ := m.Get(name, &v); ok {
		if err := json.Unmarshal([]byte(v), vs); err != nil {
			return nil, fmt.Errorf("error unmarshall %s, the data in db may be corrupted", name)
		} else {
			return vs, nil
		}
	}
	return nil, common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("%s is not found", name))
}

func GetDataSource(m kv.KeyValue, name string) (stmt *StreamStmt, err error) {
	info, err := GetDataSourceStatement(m, name)
	if err != nil {
		return nil, err
	}
	parser := NewParser(strings.NewReader(info.Statement))
	stream, err := Language.Parse(parser)
	stmt, ok := stream.(*StreamStmt)
	if !ok {
		err = fmt.Errorf("Error resolving the stream %s, the data in db may be corrupted.", name)
	}
	return
}
