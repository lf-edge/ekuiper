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

package message

const (
	FormatBinary    = "binary"
	FormatJson      = "json"
	FormatProtobuf  = "protobuf"
	FormatDelimited = "delimited"
	FormatCustom    = "custom"

	DefaultField = "self"
	MetaKey      = "__meta"
)

func IsFormatSupported(format string) bool {
	switch format {
	case FormatBinary, FormatJson, FormatProtobuf, FormatCustom, FormatDelimited:
		return true
	default:
		return false
	}
}

// Converter converts bytes & map or []map according to the schema
type Converter interface {
	Encode(d interface{}) ([]byte, error)
	Decode(b []byte) (interface{}, error)
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
