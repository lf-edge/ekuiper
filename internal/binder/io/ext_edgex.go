// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/v2/internal/io/edgex"
	edgexCon "github.com/lf-edge/ekuiper/v2/internal/io/edgex/client"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterConnection("edgex", edgexCon.GetConnection)
	modules.RegisterSource("edgex", edgex.GetSource)
	modules.RegisterSink("edgex", edgex.GetSink)
}
