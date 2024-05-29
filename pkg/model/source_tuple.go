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

package model

import (
	"time"

	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type DefaultSourceTuple struct {
	message map[string]any
	meta    map[string]any
	time    time.Time
	raw     []byte
}

// NewDefaultRawTuple creates a new DefaultSourceTuple with raw data. Use this when extend source connector
func NewDefaultRawTuple(raw []byte, meta map[string]any, ts time.Time) *DefaultSourceTuple {
	return &DefaultSourceTuple{
		meta: meta,
		time: ts,
		raw:  raw,
	}
}

func NewDefaultRawTupleIgnoreTs(raw []byte, meta map[string]any) *DefaultSourceTuple {
	return &DefaultSourceTuple{
		meta: meta,
		raw:  raw,
		time: timex.Maxtime,
	}
}

func NewDefaultSourceTuple(message map[string]any, meta map[string]any, timestamp time.Time) *DefaultSourceTuple {
	return &DefaultSourceTuple{
		message: message,
		meta:    meta,
		time:    timestamp,
	}
}

func (t *DefaultSourceTuple) Value(key, table string) (any, bool) {
	v, ok := t.message[key]
	return v, ok
}

func (t *DefaultSourceTuple) Range(f func(key string, value any) bool) {
	for k, v := range t.message {
		if !f(k, v) {
			break
		}
	}
}

func (t *DefaultSourceTuple) ToMap() map[string]any {
	return t.message
}

func (t *DefaultSourceTuple) Meta(key, table string) (any, bool) {
	v, ok := t.meta[key]
	return v, ok
}

func (t *DefaultSourceTuple) AllMeta() map[string]any {
	return t.meta
}

func (t *DefaultSourceTuple) Timestamp() time.Time {
	return t.time
}

func (t *DefaultSourceTuple) Raw() []byte {
	return t.raw
}
