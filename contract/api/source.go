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

package api

import "time"

// The source capabilities are split to several functionality
// Implementations can implement part of them and combine

// Source is the interface that wraps the basic Source method.
// The lifecycle of a source: Provision -> Connect -> Subscribe -> Close
type Source interface {
	Nodelet
	Connector
}

type BytesIngest func(ctx StreamContext, payload []byte, meta map[string]any, ts time.Time)

type BytesSource interface {
	Source
	Subscribe(ctx StreamContext, ingest BytesIngest) error
}

// TupleIngest reads in a structural data or its list.
// It supports map and []map for now
type TupleIngest func(ctx StreamContext, data any, meta map[string]any, ts time.Time)

type TupleSource interface {
	Source
	Subscribe(ctx StreamContext, ingest TupleIngest) error
}

type EOFIngest func(ctx StreamContext)

type Bounded interface {
	SetEofIngest(eof EOFIngest)
}

// Rewindable is a source feature that allows the source to rewind to a specific offset.
type Rewindable interface {
	GetOffset() (any, error)
	Rewind(offset any) error
	ResetOffset(input map[string]any) error
}

type LookupSource interface {
	// Open creates the connection to the external data source
	Open(ctx StreamContext) error
	// Configure Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties
	// read from the yaml
	Configure(datasource string, props map[string]interface{}) error
	// Lookup receive lookup values to construct the query and return query results
	Lookup(ctx StreamContext, fields []string, keys []string, values []interface{}) ([]Tuple, error)
	Closable
}
