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

// The source capabilities are split to several trait
// Implementations can implement part of them and combine

// Source is the raw interface that wraps the basic Source method. It cannot be used independently, must implement more traits.
// The lifecycle of a source: Provision -> Connect -> Subscribe/Pull -> Close
type Source interface {
	Nodelet
	Connector
}

/// Source interfaces to be implemented. With raw source interface and a selected mandatory trait

// BytesSource receives the bytes payload pushed by the external source
type BytesSource interface {
	Source
	Subscribe(ctx StreamContext, ingest BytesIngest, ingestError ErrorIngest) error
}

// TupleSource receives the non-bytes payload pushed by the external source
type TupleSource interface {
	Source
	Subscribe(ctx StreamContext, ingest TupleIngest, ingestError ErrorIngest) error
}

// PullBytesSource fetch the bytes payload in an interval from the external source. Interval property must be defined
type PullBytesSource interface {
	Source
	Pull(ctx StreamContext, trigger time.Time, ingest BytesIngest, ingestError ErrorIngest)
}

// PullTupleSource fetch the non-bytes payload in an interval from the external source. Interval property must be defined
type PullTupleSource interface {
	Source
	Pull(ctx StreamContext, trigger time.Time, ingest TupleIngest, ingestError ErrorIngest)
}

/// Other optional traits

// Bounded means the source can have an end.
type Bounded interface {
	SetEofIngest(eof EOFIngest)
}

// Rewindable is a source feature that allows the source to rewind to a specific offset.
type Rewindable interface {
	GetOffset() (any, error)
	Rewind(offset any) error
	ResetOffset(input map[string]any) error
}

// LookupSource is a source feature to query the source on demand
type LookupSource interface {
	Source
	// Lookup receive lookup values to construct the query and return query results
	Lookup(ctx StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error)
}

// LookupBytesSource looks up with the bytes payload pushed by the external source
type LookupBytesSource interface {
	Source
	// Lookup receive multiple rows of bytes
	Lookup(ctx StreamContext, fields []string, keys []string, values []any) ([][]byte, error)
}

/// helper function definition

type ErrorIngest func(ctx StreamContext, err error)
type BytesIngest func(ctx StreamContext, payload []byte, meta map[string]any, ts time.Time)

// TupleIngest reads in a structural data or its list.
// It supports map and []map for now
type TupleIngest func(ctx StreamContext, data any, meta map[string]any, ts time.Time)
type EOFIngest func(ctx StreamContext, msg string)
