// Copyright 2021 EMQ Technologies Co., Ltd.
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

package io

import (
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

var ( // init once and read only
	sourceFactories      []binder.SourceFactory
	sourceFactoriesNames []string
	sinkFactories        []binder.SinkFactory
	sinkFactoriesNames   []string
)

func init() {
	f := binder.FactoryEntry{
		Name:    "built-in",
		Factory: GetManager(),
	}
	applyFactory(f)
}

func Initialize(factories []binder.FactoryEntry) error {
	for _, f := range factories {
		applyFactory(f)
	}
	return nil
}

func applyFactory(f binder.FactoryEntry) {
	if s, ok := f.Factory.(binder.SourceFactory); ok {
		sourceFactories = append(sourceFactories, s)
		sourceFactoriesNames = append(sourceFactoriesNames, f.Name)
	}
	if s, ok := f.Factory.(binder.SinkFactory); ok {
		sinkFactories = append(sinkFactories, s)
		sinkFactoriesNames = append(sinkFactoriesNames, f.Name)
	}
}

func Source(name string) (api.Source, error) {
	e := make(errorx.MultiError)
	for i, sf := range sourceFactories {
		r, err := sf.Source(name)
		if err != nil {
			e[sourceFactoriesNames[i]] = err
		}
		if r != nil {
			return r, e.GetError()
		}
	}
	return nil, e.GetError()
}

func Sink(name string) (api.Sink, error) {
	e := make(errorx.MultiError)
	for i, sf := range sinkFactories {
		r, err := sf.Sink(name)
		if err != nil {
			e[sinkFactoriesNames[i]] = err
		}
		if r != nil {
			return r, e.GetError()
		}
	}
	return nil, e.GetError()
}
