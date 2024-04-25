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

package node

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type SinkConf struct {
	Concurrency    int      `json:"concurrency"`
	Omitempty      bool     `json:"omitIfEmpty"`
	SendSingle     bool     `json:"sendSingle"`
	DataTemplate   string   `json:"dataTemplate"`
	Format         string   `json:"format"`
	SchemaId       string   `json:"schemaId"`
	Delimiter      string   `json:"delimiter"`
	BufferLength   int      `json:"bufferLength"`
	Fields         []string `json:"fields"`
	DataField      string   `json:"dataField"`
	BatchSize      int      `json:"batchSize"`
	LingerInterval int      `json:"lingerInterval"`
	Compression    string   `json:"compression"`
	conf.SinkConf
}

func ParseConf(logger api.Logger, props map[string]any) (*SinkConf, error) {
	sconf := &SinkConf{
		Concurrency:  1,
		Omitempty:    false,
		SendSingle:   false,
		DataTemplate: "",
		SinkConf:     *conf.Config.Sink,
		BufferLength: 1024,
	}
	err := cast.MapToStruct(props, sconf)
	if err != nil {
		return nil, fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if sconf.Concurrency <= 0 {
		logger.Warnf("invalid type for concurrency property, should be positive integer but found %d", sconf.Concurrency)
		sconf.Concurrency = 1
	}
	if sconf.Format == "" {
		sconf.Format = "json"
	} else if sconf.Format != message.FormatJson && sconf.Format != message.FormatProtobuf && sconf.Format != message.FormatBinary && sconf.Format != message.FormatCustom && sconf.Format != message.FormatDelimited {
		logger.Warnf("invalid type for format property, should be json protobuf or binary but found %s", sconf.Format)
		sconf.Format = "json"
	}
	err = cast.MapToStruct(props, &sconf.SinkConf)
	if err != nil {
		return nil, fmt.Errorf("read properties %v to cache conf fail with error: %v", props, err)
	}
	if sconf.DataField == "" {
		if v, ok := props["tableDataField"]; ok {
			sconf.DataField = v.(string)
		}
	}
	if sconf.BatchSize < 0 {
		return nil, fmt.Errorf("invalid batchSize %d", sconf.BatchSize)
	}
	if sconf.LingerInterval < 0 {
		return nil, fmt.Errorf("invalid lingerInterval %d", sconf.LingerInterval)
	}
	err = sconf.SinkConf.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid cache properties: %v", err)
	}
	return sconf, err
}
