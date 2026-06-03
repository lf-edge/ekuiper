// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
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
	StreamType   StreamType // default to TypeStream

	Statement
}

type StreamField struct {
	Name string
	FieldType
	Default Literal // the DEFAULT clause is optional
}

type JsonStreamField struct {
	Type         string                      `json:"type,omitempty"`
	DefaultValue *string                     `json:"default,omitempty"`
	Items        *JsonStreamField            `json:"items,omitempty"`
	Properties   map[string]*JsonStreamField `json:"properties,omitempty"`
	HasIndex     bool                        `json:"hasIndex,omitempty"`
	Index        int                         `json:"index"`

	Selected bool `json:"selected,omitempty"`
}

func (u *StreamField) MarshalJSON() ([]byte, error) {
	var defaultValue *string

	if u.Default != nil {
		def := u.Default.String()

		// validate that the default value is compatible with the field type
		_, err := GetDefaultClause(&def, u.FieldType)
		if err != nil {
			return nil, err
		}

		defaultValue = &def
	}

	return json.Marshal(&struct {
		FieldType     interface{}
		Name          string
		DefaultClause *string `json:",omitempty"`
	}{
		FieldType:     printFieldTypeForJson(u.FieldType),
		Name:          u.Name,
		DefaultClause: defaultValue,
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
	if len(sfs) > 0 {
		result := make(map[string]*JsonStreamField, len(sfs))
		for _, sf := range sfs {
			jsonField := convertFieldType(sf.FieldType)
			if sf.Default != nil {
				defaultValue := sf.Default.String()
				jsonField.DefaultValue = &defaultValue
			}
			result[sf.Name] = jsonField
		}
		return result
	}
	return nil
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
	default:
		return &JsonStreamField{}
	}
}

func fieldsTypeFromSchema(mjsf map[string]*JsonStreamField) (StreamFields, error) {
	sfs := make(StreamFields, 0, len(mjsf))
	for k, v := range mjsf {
		ft, err := fieldTypeFromSchema(v)
		if err != nil {
			return nil, err
		}

		field := StreamField{
			Name:      k,
			FieldType: ft,
		}

		var def Literal
		def, err = fieldDefaultClauseFromSchema(v)
		if err != nil {
			return nil, err
		}

		if def != nil {
			field.Default = def
		}

		sfs = append(sfs, field)
	}
	return sfs, nil
}

func fieldDefaultClauseFromSchema(v *JsonStreamField) (Literal, error) {
	if v.DefaultValue == nil {
		return nil, nil
	}

	return getClause(*v.DefaultValue, v.Type)
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
	VERSION           string `json:"version,omitempty"`
	EXTRA             string `json:"extra,omitempty"`
	Temp              bool   `json:"temp,omitempty"`
	// for scan table only
	RETAIN_SIZE int `json:"retainSize,omitempty"`
	// for table only, to distinguish lookup & scan
	KIND string `json:"kind,omitempty"`
	// for delimited format only
	DELIMITER string `json:"delimiter,omitempty"`

	RuleID       string                      `json:"-"`
	Schema       map[string]*JsonStreamField `json:"-"`
	IsWildCard   bool                        `json:"-"`
	IsSchemaLess bool                        `json:"-"`
	StreamName   string                      `json:"-"`
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

func CheckSchemaIndex(schema map[string]*JsonStreamField) bool {
	for _, field := range schema {
		return field != nil && field.HasIndex
	}
	return false
}

func GetDefaultClause(constraintValue *string, fieldType any) (Literal, error) {
	var matchErr error

	switch t := fieldType.(type) {
	case FieldType:
		switch ft := fieldType.(type) {
		case *BasicType:
			lit, err := getClause(*constraintValue, ft.Type.String())
			if err != nil {
				return nil, err
			}
			return lit, err
		default:
			matchErr = fmt.Errorf("DEFAULT clause is not supported for %T", ft)
		}
	case string:
		lit, err := getClause(*constraintValue, t)
		if err != nil {
			return nil, err
		}
		return lit, err
	default:
		matchErr = fmt.Errorf("unsupported type %T", t)
	}

	return nil, matchErr
}

func getClause(clauseValue string, fieldType string) (Literal, error) {
	var err error

	switch fieldType {
	case BIGINT.String():
		var val int64
		if val, err = cast.ToInt64(clauseValue, cast.CONVERT_ALL); err == nil {
			return &IntegerLiteral{Val: val}, nil
		}
	case FLOAT.String():
		var val float64
		if val, err = cast.ToFloat64(clauseValue, cast.CONVERT_ALL); err == nil {
			return &NumberLiteral{Val: val}, nil
		}
	case STRINGS.String():
		return &StringLiteral{Val: clauseValue}, nil
	case BOOLEAN.String():
		var val bool
		if val, err = cast.ToBool(clauseValue, cast.CONVERT_ALL); err == nil {
			return &BooleanLiteral{Val: val}, nil
		}
	default:
		err = fmt.Errorf("DEFAULT clause is not supported for %s", fieldType)
	}

	return nil, err
}
