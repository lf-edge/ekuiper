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

	"github.com/lf-edge/ekuiper/contract/v2/api"
)

// DefaultMessage is a valuer that substitutes values for the mapped interface. It is the basic type for data events.
type DefaultMessage map[string]interface{}

func (m DefaultMessage) Get(key string) (value any, ok bool) {
	v, o := m[key]
	return v, o
}

func (m DefaultMessage) Range(f func(key string, value any) bool) {
	for k, v := range m {
		exit := f(k, v)
		if exit {
			break
		}
	}
}

func (m DefaultMessage) ToMap() map[string]any {
	return m
}

var _ api.ReadonlyMessage = DefaultMessage(nil)

type DefaultSourceTuple struct {
	message api.ReadonlyMessage
	meta    api.ReadonlyMessage
	time    time.Time
	raw     []byte
}

// NewDefaultRawTuple creates a new DefaultSourceTuple with raw data. Use this when extend source connector
func NewDefaultRawTuple(raw []byte, meta api.ReadonlyMessage, ts time.Time) *DefaultSourceTuple {
	return &DefaultSourceTuple{
		meta: meta,
		time: ts,
		raw:  raw,
	}
}

func NewDefaultSourceTuple(message api.ReadonlyMessage, meta api.ReadonlyMessage, timestamp time.Time) *DefaultSourceTuple {
	return &DefaultSourceTuple{
		message: message,
		meta:    meta,
		time:    timestamp,
	}
}

func (t *DefaultSourceTuple) Message() api.ReadonlyMessage {
	return t.message
}

func (t *DefaultSourceTuple) Meta() api.ReadonlyMessage {
	return t.meta
}

func (t *DefaultSourceTuple) Timestamp() time.Time {
	return t.time
}

func (t *DefaultSourceTuple) Raw() []byte {
	return t.raw
}
