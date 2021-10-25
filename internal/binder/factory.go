// Copyright 2022 EMQ Technologies Co., Ltd.
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

package binder

import "github.com/lf-edge/ekuiper/pkg/api"

type SourceFactory interface {
	Source(name string) (api.Source, error)
}

type SinkFactory interface {
	Sink(name string) (api.Sink, error)
}

type FuncFactory interface {
	Function(name string) (api.Function, error)
	// HasFunctionSet Some functions are bundled together into a plugin which shares the same json file.
	// This function can return if the function set name exists.
	HasFunctionSet(funcName string) bool
	// ConvName Convert the name of the function usually to lowercase.
	// This is only be used when parsing the SQL statement.
	ConvName(funcName string) (string, bool)
}

type FactoryEntry struct {
	Name    string
	Factory interface{}
	Weight  int // bigger weight will be initialized first
}

type Entries []FactoryEntry

func (e Entries) Len() int {
	return len(e)
}

func (e Entries) Less(i, j int) bool {
	return e[i].Weight > e[j].Weight
}

func (e Entries) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
