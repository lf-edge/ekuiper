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
	"github.com/lf-edge/ekuiper/pkg/api"
)

type (
	NewSourceFunc       func() api.Source
	NewLookupSourceFunc func() api.LookupSource
	NewSinkFunc         func() api.Sink
)

var (
	Sources       = map[string]NewSourceFunc{}
	Sinks         = map[string]NewSinkFunc{}
	LookupSources = map[string]NewLookupSourceFunc{}
)

func RegisterSource(name string, f NewSourceFunc) {
	Sources[name] = f
}

func RegisterSink(name string, f NewSinkFunc) {
	Sinks[name] = f
}

func RegisterLookupSource(name string, f NewLookupSourceFunc) {
	LookupSources[name] = f
}
