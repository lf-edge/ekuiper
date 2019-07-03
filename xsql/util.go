package xsql

import "bytes"

func PrintFieldType(ft FieldType, buff *bytes.Buffer) {
	switch t := ft.(type) {
	case *BasicType:
		buff.WriteString(t.Type.String())
	case *ArrayType:
		buff.WriteString("array(")
		if t.FieldType != nil {
			PrintFieldType(t.FieldType, buff)
		}else{
			buff.WriteString(t.Type.String())
		}
		buff.WriteString(")")
	case *RecType:
		buff.WriteString("struct(")
		isFirst := true
		for _, f := range t.StreamFields {
			if isFirst{
				isFirst = false
			}else{
				buff.WriteString(", ")
			}
			buff.WriteString(f.Name + " ")
			PrintFieldType(f.FieldType, buff)
		}
		buff.WriteString(")")
	}
}