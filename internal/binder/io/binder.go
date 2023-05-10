// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"

	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/pkg/api"
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
	var errs error
	for i, sf := range sourceFactories {
		r, err := sf.Source(name)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("%s:%v", sourceFactoriesNames[i], err))
		}
		if r != nil {
			return r, errs
		}
	}
	return nil, errs
}

func GetSourcePlugin(name string) (plugin.EXTENSION_TYPE, string, string) {
	for _, sf := range sourceFactories {
		t, s1, s2 := sf.SourcePluginInfo(name)
		if t == plugin.NONE_EXTENSION {
			continue
		}
		return t, s1, s2
	}
	return plugin.NONE_EXTENSION, "", ""
}

func Sink(name string) (api.Sink, error) {
	var errs error
	for i, sf := range sinkFactories {
		r, err := sf.Sink(name)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("%s:%v", sinkFactoriesNames[i], err))
		}
		if r != nil {
			return r, errs
		}
	}
	return nil, errs
}

func GetSinkPlugin(name string) (plugin.EXTENSION_TYPE, string, string) {
	for _, sf := range sinkFactories {
		t, s1, s2 := sf.SinkPluginInfo(name)
		if t == plugin.NONE_EXTENSION {
			continue
		}
		return t, s1, s2
	}
	return plugin.NONE_EXTENSION, "", ""
}

func LookupSource(name string) (api.LookupSource, error) {
	var errs error
	for i, sf := range sourceFactories {
		r, err := sf.LookupSource(name)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("%s:%v", sourceFactoriesNames[i], err))
		}
		if r != nil {
			return r, errs
		}
	}
	if errs == nil {
		errs = fmt.Errorf("lookup source type %s not found", name)
	}
	return nil, errs
}
