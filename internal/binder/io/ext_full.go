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

//go:build full

package io

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/image"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/influx"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/influx2"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/kafka"
	sql2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/video"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterSource("video", func() api.Source { return video.GetSource() })
	modules.RegisterSource("kafka", func() api.Source { return kafka.GetSource() })
	modules.RegisterSink("kafka", func() api.Sink { return kafka.GetSink() })
	modules.RegisterSink("image", func() api.Sink { return image.GetSink() })
	modules.RegisterSink("influx", func() api.Sink { return influx.GetSink() })
	modules.RegisterSink("influx2", func() api.Sink { return influx2.GetSink() })
	modules.RegisterSource("sql", sql2.GetSource)
	modules.RegisterLookupSource("sql", sql2.GetLookupSource)
	modules.RegisterSink("sql", sql2.GetSink)
}
