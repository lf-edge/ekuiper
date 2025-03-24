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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

const (
	FormatBinary     = "binary"
	FormatJson       = "json"
	FormatProtobuf   = "protobuf"
	FormatDelimited  = "delimited"
	FormatUrlEncoded = "urlencoded"
	FormatXML        = "xml"
	FormatCustom     = "custom"

	DefaultField = "self"
	MetaKey      = "__meta"
)

// Converter converts bytes & map or []map according to the schema
type Converter interface {
	Encode(ctx api.StreamContext, d any) ([]byte, error)
	Decode(ctx api.StreamContext, b []byte) (any, error)
}

// ConvertWriter encode like a writer (streaming)
type ConvertWriter interface {
	// New init a new container "file" in buffer
	New(ctx api.StreamContext) error
	Write(ctx api.StreamContext, d any) error
	Flush(ctx api.StreamContext) ([]byte, error)
}

// PartialDecoder decodes a field partially
type PartialDecoder interface {
	DecodeField(ctx api.StreamContext, b []byte, f string) (any, error)
}

type SchemaResetAbleConverter interface {
	ResetSchema(schema map[string]*ast.JsonStreamField)
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

// Encryptor encrypts bytes
type Encryptor interface {
	Encrypt([]byte) ([]byte, error)
}
