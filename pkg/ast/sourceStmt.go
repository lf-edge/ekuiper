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

func (u *StreamField) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		FieldType interface{}
		Name      string
	}{
		FieldType: printFieldTypeForJson(u.FieldType),
		Name:      u.Name,
	})
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
