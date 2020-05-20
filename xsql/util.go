package xsql

import (
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

func LowercaseKeyMap(m map[string]interface{}, orig map[string]interface{}) map[string]interface{} {
	m1 := make(map[string]interface{})
	for k, v := range m {
		if m2, ok := v.(map[string]interface{}); ok {
			o1 := make(map[string]interface{})
			orig[k] = o1
			m1[strings.ToLower(k)] = LowercaseKeyMap(m2, o1)
		} else {
			m1[strings.ToLower(k)] = v
			orig[k] = nil
		}
	}
	return m1
}

//TODO To handle nested types?
func GetOriginalKey(lkey string, okeys map[string]interface{}) (bool, string){
	for k, _ := range okeys {
		if strings.ToLower(k) == lkey {
			return true, k
		}
	}
	return false, ""
}