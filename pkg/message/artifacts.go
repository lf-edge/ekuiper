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

package message

import (
	"github.com/lf-edge/ekuiper/pkg/ast"
)

const (
	FormatBinary    = "binary"
	FormatJson      = "json"
	FormatProtobuf  = "protobuf"
	FormatDelimited = "delimited"
	FormatCustom    = "custom"

	DefaultField = "self"
	MetaKey      = "__meta"
)

// Converter converts bytes & map or []map according to the schema
type Converter interface {
	Encode(d interface{}) ([]byte, error)
	Decode(b []byte) (interface{}, error)
}

// ConverterProvider The format, schema information are passed in by stream options
// The columns information is defined in the source side, like file source
type ConverterProvider func(schemaFileName string, SchemaMessageName string, delimiter string) (Converter, error)

type SchemaMergeAbleConverter interface {
	MergeSchema(key, datasource string, newSchema map[string]*ast.JsonStreamField, isWildcard bool) error
	DetachSchema(key string) error
}

type ColumnSetter interface {
	SetColumns([]string)
}

type SchemaProvider interface {
	GetSchemaJson() string
}

// Compressor compresses and decompresses bytes
type Compressor interface {
	Compress([]byte) ([]byte, error)
}

type Decompressor interface {
	Decompress([]byte) ([]byte, error)
}
