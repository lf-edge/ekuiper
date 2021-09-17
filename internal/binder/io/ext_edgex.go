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

//go:build edgex
// +build edgex

package io

import (
	"github.com/lf-edge/ekuiper/internal/topo/sink"
	"github.com/lf-edge/ekuiper/internal/topo/source"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func init() {
	sources["edgex"] = func() api.Source { return &source.EdgexSource{} }
	sinks["edgex"] = func() api.Sink { return &sink.EdgexMsgBusSink{} }
}
