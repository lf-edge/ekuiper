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

package collector

import (
	"errors"
	"github.com/lf-edge/ekuiper/pkg/api"
)

// CollectorFunc is a function used to colllect
// incoming stream data. It can be used as a
// stream sink.
type CollectorFunc func(api.StreamContext, interface{}) error

// FuncCollector is a colletor that uses a function
// to collect data.  The specified function must be
// of type:
//
//	CollectorFunc
type FuncCollector struct {
	f CollectorFunc
}

// Func creates a new value *FuncCollector that
// will use the specified function parameter to
// collect streaming data.
func Func(f CollectorFunc) *FuncCollector {
	return &FuncCollector{f: f}
}

func (c *FuncCollector) Configure(props map[string]interface{}) error {
	//do nothing
	return nil
}

// Open is the starting point that starts the collector
func (c *FuncCollector) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Infoln("Opening func collector")

	if c.f == nil {
		return errors.New("func collector missing function")
	}
	return nil
}

func (c *FuncCollector) Collect(ctx api.StreamContext, item interface{}) error {
	return c.f(ctx, item)
}

func (c *FuncCollector) Close(api.StreamContext) error {
	return nil
}
