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

package sink

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"
)

type NopSink struct {
	log bool
}

func (ns *NopSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	var log bool
	l, ok := configs["log"]
	if ok {
		log = l.(bool)
	}
	ns.log = log
	return nil
}

func (ns *NopSink) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (ns *NopSink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	logger := ctx.GetLogger()
	if ns.log {
		logger.Infof("%s", item.Raw())
	}
	return nil
}

func (ns *NopSink) Close(ctx api.StreamContext) error {
	return nil
}

func GetSink() api.Sink {
	return &NopSink{}
}

var _ api.BytesCollector = &NopSink{}
