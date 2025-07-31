// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package modules

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

var Converters = map[string]ConverterProvider{}

// ConverterProvider
// - schemaId: the id for the schema
// - logicalSchema: the default schema
// - props: extended properties. Implementation can read and handle it.
type ConverterProvider func(ctx api.StreamContext, schemaId string, logicalSchema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error)

// RegisterConverter registers a converter with the given name.
func RegisterConverter(name string, provider ConverterProvider) {
	Converters[name] = provider
}

func IsFormatSupported(format string) bool {
	_, ok := Converters[format]
	return ok
}

// ConvertWriters are sink converter to use together with batch
var ConvertWriters = map[string]ConvertWriterProvider{}

type ConvertWriterProvider func(ctx api.StreamContext, schemaId string, logicalSchema map[string]*ast.JsonStreamField, props map[string]any) (message.ConvertWriter, error)

func RegisterWriterConverter(name string, provider ConvertWriterProvider) {
	ConvertWriters[name] = provider
}

// Merger is used to merge multiple frames. It is currently called by rate limiter only
type Merger interface {
	// Merging is called when read in a new frame
	Merging(ctx api.StreamContext, b []byte) error
	// Trigger is called when rate limiter is trigger to send out a message
	Trigger(ctx api.StreamContext) ([]any, bool)
}

type MergerProvider func(ctx api.StreamContext, payloadSchema string, logicalSchema map[string]*ast.JsonStreamField) (Merger, error)

// Mergers list, the key is format + payload format such as "jsoncan"
var Mergers = map[string]MergerProvider{}

// RegisterMerger registers a merger with the format name and payload format name such as "jsoncan"
func RegisterMerger(name string, provider MergerProvider) {
	Mergers[name] = provider
}

var ConverterSchemas = map[string]string{}

func RegisterConverterSchemas(name string, schemaType string) {
	ConverterSchemas[name] = schemaType
}
