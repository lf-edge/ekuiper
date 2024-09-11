// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/contract/v2/api"
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

func (c *FuncCollector) Provision(ctx api.StreamContext, configs map[string]any) error {
	// do nothing
	return nil
}

func (c *FuncCollector) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	ctx.GetLogger().Info("Opening func collector")
	if c.f == nil {
		err := errors.New("func collector missing function")
		sch(api.ConnectionDisconnected, err.Error())
		return err
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (c *FuncCollector) Collect(ctx api.StreamContext, item api.RawTuple) error {
	return c.f(ctx, item.Raw())
}

func (c *FuncCollector) Close(api.StreamContext) error {
	return nil
}

var _ api.BytesCollector = &FuncCollector{}
