// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"io"

	"github.com/lf-edge/ekuiper/contract/v2/api"
)

type RollHook interface {
	Provision(ctx api.StreamContext, props map[string]any) error
	RollDone(ctx api.StreamContext, filePath string) error
	api.Closable
}

type RollHookProvider func() RollHook

var fileRollHooks = map[string]RollHookProvider{}

func RegisterFileRollHook(name string, provider RollHookProvider) {
	fileRollHooks[name] = provider
}

func GetFileRollHook(name string) (RollHook, bool) {
	if p, ok := fileRollHooks[name]; ok {
		return p(), ok
	}
	return nil, false
}

// FileStreamReader reads a type of file line by line. Avoid to load the full file
// If need to load full file, just extend converter to decode the full bytes
type FileStreamReader interface {
	// Provision Set up the static properties
	Provision(ctx api.StreamContext, props map[string]any) error
	// Bind set the file stream. Make sure the previous read has done
	Bind(ctx api.StreamContext, fileStream io.Reader, maxSize int) error
	// Read the next record. Returns EOF when the input has reached its end.
	Read(ctx api.StreamContext) (any, error)
	// IsBytesReader If is bytes reader, Read must return []byte, otherwise return map or []map
	IsBytesReader() bool
	api.Closable
}

type FileStreamReaderProvider func(ctx api.StreamContext) FileStreamReader

var fileStreamReaders = map[string]FileStreamReaderProvider{}

func RegisterFileStreamReader(name string, provider FileStreamReaderProvider) {
	fileStreamReaders[name] = provider
}

func RegisterFileStreamReaderAlias(alias string, ref string) {
	fileStreamReaders[alias] = fileStreamReaders[ref]
}

func GetFileStreamReader(ctx api.StreamContext, name string) (FileStreamReader, bool) {
	if p, ok := fileStreamReaders[name]; ok {
		return p(ctx), true
	}
	return nil, false
}

type FileStreamDecorator interface {
	// Provision Set up the static properties
	Provision(ctx api.StreamContext, props map[string]any) error
	// ReadMeta Read the metadata from the file source, and save in the decorator itself
	// It will receive lines, when receiving EOF, there will be no more lines.
	ReadMeta(ctx api.StreamContext, line []byte)
	// Decorate the file source
	Decorate(ctx api.StreamContext, data any) any
}

type FileStreamDecoratorProvider func(ctx api.StreamContext) FileStreamDecorator

var fileStreamDecorators = map[string]FileStreamDecoratorProvider{}

func RegisterFileStreamDecorator(name string, provider FileStreamDecoratorProvider) {
	fileStreamDecorators[name] = provider
}

func GetFileStreamDecorator(ctx api.StreamContext, name string) (FileStreamDecorator, bool) {
	if p, ok := fileStreamDecorators[name]; ok {
		return p(ctx), true
	}
	return nil, false
}
