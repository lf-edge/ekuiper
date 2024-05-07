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

import (
	"time"
)

// SinkTuple is an interface of the below interfaces
type SinkTuple interface {
	ReadonlyMessage
}

type SinkRawTuple interface {
	HasRaw
}

// ReadonlyMessage Message is the interface that wraps each record.
// Use this interface to exchange data between different components.
// It is used in sink
type ReadonlyMessage interface {
	Value(key, table string) (any, bool)
	All(table string) (map[string]any, bool)
}

type MetaInfo interface {
	Meta(key, table string) (any, bool)
	Created() time.Time
	AllMeta() map[string]any
}

type HasRaw interface {
	Raw() []byte
}

type HasDynamicProps interface {
	// DynamicProps return the transformed dynamic properties (typically in sink).
	// The transformation should be done in transform op
	DynamicProps(template string) (string, error)
}

type SinkTupleList interface {
	RangeOfTuples(f func(index int, tuple SinkTuple) bool)
	Len() int
}
