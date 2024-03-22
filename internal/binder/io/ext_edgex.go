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

//go:build edgex || full

package io

import (
	"github.com/lf-edge/ekuiper/internal/io/edgex"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/modules"
)

func init() {
	modules.RegisterSource("edgex", func() api.Source { return &edgex.EdgexSource{} })
	modules.RegisterSink("edgex", func() api.Sink { return &edgex.EdgexMsgBusSink{} })
}
