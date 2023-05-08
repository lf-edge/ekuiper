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

package sink

import (
	"github.com/lf-edge/ekuiper/pkg/api"
)

type NopSink struct {
	log bool
}

func (ns *NopSink) Configure(ps map[string]interface{}) error {
	var log bool
	l, ok := ps["log"]
	if ok {
		log = l.(bool)
	}
	ns.log = log
	return nil
}

func (ns *NopSink) Open(ctx api.StreamContext) error {
	return nil
}

func (ns *NopSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if ns.log {
		logger.Infof("%s", item)
	}
	return nil
}

func (ns *NopSink) Close(ctx api.StreamContext) error {
	return nil
}
