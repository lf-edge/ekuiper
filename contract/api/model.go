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

// ReadonlyMessage Message is the interface that wraps each record.
// Use this interface to exchange data between different components.
// It is used in sink
type ReadonlyMessage interface {
	Get(key string) (value any, ok bool)
	Range(f func(key string, value any) bool)
	// ToMap todo remove after eliminate map
	ToMap() map[string]any
}

type MetaInfo interface {
	Meta() ReadonlyMessage
	Timestamp() time.Time
}

// Tuple is the record passing in source and sink
type Tuple interface {
	Message() ReadonlyMessage
	MetaInfo
}

type RawTuple interface {
	Raw() []byte
	MetaInfo
}
