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
	image "github.com/lf-edge/ekuiper/extensions/sinks/image/ext"
	influx "github.com/lf-edge/ekuiper/extensions/sinks/influx/ext"
	influx2 "github.com/lf-edge/ekuiper/extensions/sinks/influx2/ext"
	kafka "github.com/lf-edge/ekuiper/extensions/sinks/kafka/ext"
	sqlSink "github.com/lf-edge/ekuiper/extensions/sinks/sql/ext"
	kafkaSrc "github.com/lf-edge/ekuiper/extensions/sources/kafka/ext"
	random "github.com/lf-edge/ekuiper/extensions/sources/random/ext"
	sql "github.com/lf-edge/ekuiper/extensions/sources/sql/ext"
	video "github.com/lf-edge/ekuiper/extensions/sources/video/ext"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/modules"
)

func init() {
	modules.RegisterSource("random", func() api.Source { return random.GetSource() })
	modules.RegisterSource("video", func() api.Source { return video.GetSource() })
	modules.RegisterSource("sql", func() api.Source { return sql.GetSource() })
	modules.RegisterSource("kafka", func() api.Source { return kafkaSrc.GetSource() })
	modules.RegisterLookupSource("sql", func() api.LookupSource { return sql.GetLookup() })
	modules.RegisterSink("image", func() api.Sink { return image.GetSink() })
	modules.RegisterSink("influx", func() api.Sink { return influx.GetSink() })
	modules.RegisterSink("influx2", func() api.Sink { return influx2.GetSink() })
	modules.RegisterSink("kafka", func() api.Sink { return kafka.GetSink() })
	modules.RegisterSink("sql", func() api.Sink { return sqlSink.GetSink() })
	// Do not include zmq/tdengine because it is not supported for all versions
	// sinks["tdengine"] = func() api.Sink { return tdengine.GetSink() }
	// sinks["zmq"] = func() api.Sink { return zmqSink.GetSink() }
	// sources["zmq"] = func() api.Source { return zmq.GetSource() }
}
