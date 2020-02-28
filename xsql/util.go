package xsql

import (
	"bytes"
	"strings"
)

func PrintFieldType(ft FieldType, buff *bytes.Buffer) {
	switch t := ft.(type) {
	case *BasicType:
		buff.WriteString(t.Type.String())
	case *ArrayType:
		buff.WriteString("array(")
		if t.FieldType != nil {
			PrintFieldType(t.FieldType, buff)
		} else {
			buff.WriteString(t.Type.String())
		}
		buff.WriteString(")")
	case *RecType:
		buff.WriteString("struct(")
		isFirst := true
		for _, f := range t.StreamFields {
			if isFirst {
				isFirst = false
			} else {
				buff.WriteString(", ")
			}
			buff.WriteString(f.Name + " ")
			PrintFieldType(f.FieldType, buff)
		}
		buff.WriteString(")")
	}
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
