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

//go:build full

package io

import (
	image "github.com/lf-edge/ekuiper/extensions/sinks/image/ext"
	influx "github.com/lf-edge/ekuiper/extensions/sinks/influx/ext"
	influx2 "github.com/lf-edge/ekuiper/extensions/sinks/influx2/ext"
	kafka "github.com/lf-edge/ekuiper/extensions/sinks/kafka/ext"
	sqlSink "github.com/lf-edge/ekuiper/extensions/sinks/sql/ext"
	random "github.com/lf-edge/ekuiper/extensions/sources/random/ext"
	sql "github.com/lf-edge/ekuiper/extensions/sources/sql/ext"
	video "github.com/lf-edge/ekuiper/extensions/sources/video/ext"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func init() {
	sources["random"] = func() api.Source { return random.GetSource() }
	sources["video"] = func() api.Source { return video.GetSource() }
	sources["sql"] = func() api.Source { return sql.GetSource() }
	lookupSources["sql"] = func() api.LookupSource { return sql.GetLookup() }
	sinks["image"] = func() api.Sink { return image.GetSink() }
	sinks["influx"] = func() api.Sink { return influx.GetSink() }
	sinks["influx2"] = func() api.Sink { return influx2.GetSink() }
	sinks["kafka"] = func() api.Sink { return kafka.GetSink() }
	sinks["sql"] = func() api.Sink { return sqlSink.GetSink() }
	// Do not include zmq/tdengine because it is not supported for all versions
	// sinks["tdengine"] = func() api.Sink { return tdengine.GetSink() }
	// sinks["zmq"] = func() api.Sink { return zmqSink.GetSink() }
	// sources["zmq"] = func() api.Source { return zmq.GetSource() }
}
