// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package ast

import (
	"encoding/json"
	"fmt"
)

const (
	TypeStream StreamType = iota
	TypeTable
)

var StreamTypeMap = map[StreamType]string{
	TypeStream: "stream",
	TypeTable:  "table",
}

const (
	StreamKindLookup = "lookup"
	StreamKindScan   = "scan"
)

type StreamType int

type StreamStmt struct {
	Name         StreamName
	StreamFields StreamFields
	Options      *Options
	StreamType   StreamType //default to TypeStream

	Statement
}

type StreamField struct {
	Name string
	FieldType
}

type JsonStreamField struct {
	Type       string                      `json:"type"`
	Items      *JsonStreamField            `json:"items,omitempty"`
	Properties map[string]*JsonStreamField `json:"properties,omitempty"`
}

func (u *StreamField) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		FieldType interface{}
		Name      string
	}{
		FieldType: printFieldTypeForJson(u.FieldType),
		Name:      u.Name,
	})
}

// UnmarshalJSON The json format follows json schema
func (sf *StreamFields) UnmarshalJSON(data []byte) error {
	temp := map[string]*JsonStreamField{}
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}
	return sf.UnmarshalFromMap(temp)
}

func (sf *StreamFields) UnmarshalFromMap(data map[string]*JsonStreamField) error {
	t, err := fieldsTypeFromSchema(data)
	if err != nil {
		return err
	}
	*sf = t
	return nil
}
func (sf *StreamFields) ToJsonSchema() map[string]*JsonStreamField {
	return convertSchema(*sf)
}

func convertSchema(sfs StreamFields) map[string]*JsonStreamField {
	result := make(map[string]*JsonStreamField, len(sfs))
	for _, sf := range sfs {
		result[sf.Name] = convertFieldType(sf.FieldType)
	}
	return result
}

func convertFieldType(sf FieldType) *JsonStreamField {
	switch t := sf.(type) {
	case *BasicType:
		return &JsonStreamField{
			Type: t.Type.String(),
		}
	case *ArrayType:
		var items *JsonStreamField
		switch t.Type {
		case ARRAY, STRUCT:
			items = convertFieldType(t.FieldType)
		default:
			items = &JsonStreamField{
				Type: t.Type.String(),
			}
		}
		return &JsonStreamField{
			Type:  "array",
			Items: items,
		}
	case *RecType:
		return &JsonStreamField{
			Type:       "struct",
			Properties: convertSchema(t.StreamFields),
		}
	default: // should never happen
		return nil
	}
}

func fieldsTypeFromSchema(mjsf map[string]*JsonStreamField) (StreamFields, error) {
	sfs := make(StreamFields, 0, len(mjsf))
	for k, v := range mjsf {
		ft, err := fieldTypeFromSchema(v)
		if err != nil {
			return nil, err
		}
		sfs = append(sfs, StreamField{
			Name:      k,
			FieldType: ft,
		})
	}
	return sfs, nil
}

func fieldTypeFromSchema(v *JsonStreamField) (FieldType, error) {
	var ft FieldType
	switch v.Type {
	case "array":
		if v.Items == nil {
			return nil, fmt.Errorf("array field type should have items")
		}
		itemType, err := fieldTypeFromSchema(v.Items)
		if err != nil {
			return nil, fmt.Errorf("invalid array field type: %v", err)
		}
		switch t := itemType.(type) {
		case *BasicType:
			ft = &ArrayType{
				Type: t.Type,
			}
		case *RecType:
			ft = &ArrayType{
				Type:      STRUCT,
				FieldType: t,
			}
		case *ArrayType:
			ft = &ArrayType{
				Type:      ARRAY,
				FieldType: t,
			}
		}
	case "struct":
		if v.Properties == nil {
			return nil, fmt.Errorf("struct field type should have properties")
		}
		sfs, err := fieldsTypeFromSchema(v.Properties)
		if err != nil {
			return nil, fmt.Errorf("invalid struct field type: %v", err)
		}
		ft = &RecType{StreamFields: sfs}
	case "bigint":
		ft = &BasicType{Type: BIGINT}
	case "float":
		ft = &BasicType{Type: FLOAT}
	case "string":
		ft = &BasicType{Type: STRINGS}
	case "bytea":
		ft = &BasicType{Type: BYTEA}
	case "datetime":
		ft = &BasicType{Type: DATETIME}
	case "boolean":
		ft = &BasicType{Type: BOOLEAN}
	default:
		return nil, fmt.Errorf("unsupported type %s", v.Type)
	}
	return ft, nil
}

type StreamFields []StreamField

type FieldType interface {
	fieldType()
}

type BasicType struct {
	Type DataType
	FieldType
}

type ArrayType struct {
	Type DataType
	FieldType
}

type RecType struct {
	StreamFields StreamFields
	FieldType
}

// Options The stream AST tree
type Options struct {
	DATASOURCE        string `json:"datasource,omitempty"`
	KEY               string `json:"key,omitempty"`
	FORMAT            string `json:"format,omitempty"`
	CONF_KEY          string `json:"confKey,omitempty"`
	TYPE              string `json:"type,omitempty"`
	STRICT_VALIDATION bool   `json:"strictValidation,omitempty"`
	TIMESTAMP         string `json:"timestamp,omitempty"`
	TIMESTAMP_FORMAT  string `json:"timestampFormat,omitempty"`
	SHARED            bool   `json:"shared,omitempty"`
	SCHEMAID          string `json:"schemaid,omitempty"`
	// for scan table only
	RETAIN_SIZE int `json:"retainSize,omitempty"`
	// for table only, to distinguish lookup & scan
	KIND string `json:"kind,omitempty"`
}

func (o Options) node() {}

type ShowStreamsStatement struct {
	Statement
}

type DescribeStreamStatement struct {
	Name string

	Statement
}

type ExplainStreamStatement struct {
	Name string

	Statement
}

type DropStreamStatement struct {
	Name string

	Statement
}

func (dss *DescribeStreamStatement) GetName() string { return dss.Name }

func (ess *ExplainStreamStatement) GetName() string { return ess.Name }

func (dss *DropStreamStatement) GetName() string { return dss.Name }

type ShowTablesStatement struct {
	Statement
}

type DescribeTableStatement struct {
	Name string

	Statement
}

type ExplainTableStatement struct {
	Name string

	Statement
}

type DropTableStatement struct {
	Name string

	Statement
}

func (dss *DescribeTableStatement) GetName() string { return dss.Name }
func (ess *ExplainTableStatement) GetName() string  { return ess.Name }
func (dss *DropTableStatement) GetName() string     { return dss.Name }

func printFieldTypeForJson(ft FieldType) (result interface{}) {
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
