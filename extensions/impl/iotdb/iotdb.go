// Copyright 2026 Timecho Limited
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

package iotdb

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// writer is the strategy interface implemented by tree/table specific writers.
type writer interface {
	connect(ctx api.StreamContext, conf *iotdbConfig) error
	write(ctx api.StreamContext, data []map[string]any) error
	close() error
}

// iotdbSink is the IoTDB sink that delegates writes to a model-specific writer.
type iotdbSink struct {
	conf   iotdbConfig
	writer writer
}

func (s *iotdbSink) Provision(ctx api.StreamContext, props map[string]any) error {
	s.conf = iotdbConfig{}
	if err := cast.MapToStruct(props, &s.conf); err != nil {
		return fmt.Errorf("error configuring iotdb sink: %s", err)
	}
	s.conf.applyDefaults()
	if err := s.conf.validate(); err != nil {
		return err
	}
	switch s.conf.Model {
	case modelTree:
		s.writer = &treeWriter{}
	case modelTable:
		s.writer = &tableWriter{}
	default:
		// unreachable: validate already rejected other values
		return fmt.Errorf("unsupported model %q", s.conf.Model)
	}
	return nil
}

func (s *iotdbSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	err := s.writer.connect(ctx, &s.conf)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return err
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (s *iotdbSink) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	return s.collect(ctx, item.ToMap())
}

func (s *iotdbSink) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	return s.collect(ctx, items.ToMaps())
}

func (s *iotdbSink) collect(ctx api.StreamContext, data any) error {
	maps, err := toMapList(data)
	if err != nil {
		return err
	}
	if len(maps) == 0 {
		return nil
	}
	if err := s.writer.write(ctx, maps); err != nil {
		ctx.GetLogger().Errorf("iotdb sink write error: %v", err)
		return errorx.NewIOErr(fmt.Sprintf("iotdb sink fails to send out the data: %v", err))
	}
	ctx.GetLogger().Debug("insert data into iotdb success")
	return nil
}

func (s *iotdbSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("iotdb sink close")
	if s.writer != nil {
		return s.writer.close()
	}
	return nil
}

// toMapList normalizes data into []map[string]any.
func toMapList(data any) ([]map[string]any, error) {
	switch dd := data.(type) {
	case map[string]any:
		return []map[string]any{dd}, nil
	case []map[string]any:
		return dd, nil
	default:
		return nil, fmt.Errorf("iotdb sink needs map or []map, but received unsupported data %T", dd)
	}
}

// GetSink returns a new IoTDB sink instance.
func GetSink() api.Sink {
	return &iotdbSink{}
}

// Info implements model.SinkInfoNode to declare sink capabilities.
func (s *iotdbSink) Info() model.SinkInfo {
	return model.SinkInfo{
		HasFields: true,
	}
}

// interface compliance check
var (
	_ api.TupleCollector = &iotdbSink{}
	_ model.SinkInfoNode = &iotdbSink{}
)
