// Copyright 2023 EMQ Technologies Co., Ltd.
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

import "github.com/lf-edge/ekuiper/pkg/api"

func ReceiveTuples(ctx api.StreamContext, consumer chan<- api.SourceTuple, tuples []api.SourceTuple) {
	for _, t := range tuples {
		select {
		case consumer <- t:
			ctx.GetLogger().Debugf("send data to source node")
		case <-ctx.Done():
			return
		}
	}
}
