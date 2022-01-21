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

package function

import (
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

var ( // init once and read only
	funcFactories      []binder.FuncFactory
	funcFactoriesNames []string
)

type FuncType int

const (
	FuncTypeUnknown FuncType = iota - 1
	FuncTypeScalar
	FuncTypeAgg
)

func init() {
	f := binder.FactoryEntry{
		Name:    "built-in",
		Factory: GetManager(),
	}
	applyFactory(f)
}

// Initialize Only call once when server starts
func Initialize(factories []binder.FactoryEntry) error {
	for _, f := range factories {
		applyFactory(f)
	}
	return nil
}

func applyFactory(f binder.FactoryEntry) {
	if s, ok := f.Factory.(binder.FuncFactory); ok {
		funcFactories = append(funcFactories, s)
		funcFactoriesNames = append(funcFactoriesNames, f.Name)
	}
}

func Function(name string) (api.Function, error) {
	e := make(errorx.MultiError)
	for i, sf := range funcFactories {
		r, err := sf.Function(name)
		if err != nil {
			e[funcFactoriesNames[i]] = err
		}
		if r != nil {
			return r, e.GetError()
		}
	}
	return nil, e.GetError()
}

func HasFunctionSet(name string) bool {
	for _, sf := range funcFactories {
		r := sf.HasFunctionSet(name)
		if r {
			return r
		}
	}
	return false
}

func ConvName(name string) (string, bool) {
	for _, sf := range funcFactories {
		r, ok := sf.ConvName(name)
		if ok {
			return r, ok
		}
	}
	return name, false
}

type multiAggFunc interface {
	GetFuncType(name string) FuncType
}

func IsAggFunc(funcName string) bool {
	f, _ := Function(funcName)
	if f != nil {
		if mf, ok := f.(multiAggFunc); ok {
			return mf.GetFuncType(funcName) == FuncTypeAgg
		} else {
			return f.IsAggregate()
		}
	}
	return false
}
