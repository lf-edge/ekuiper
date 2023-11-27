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

// Methods to transform data to influxdb line protocol

package influx2

import (
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func (m *influxSink2) transformLines(ctx api.StreamContext, dd any) ([]string, error) {
	var (
		lines []string
		err   error
	)
	switch dd := dd.(type) {
	case map[string]any:
		ctx.GetLogger().Debugf("influx2 sink receive data %v", dd)
		mm, err := m.transformToLine(ctx, dd)
		if err != nil {
			return nil, err
		}
		lines = append(lines, mm)
	case []map[string]any:
		ctx.GetLogger().Debugf("influx2 sink receive data %v", dd)
		if m.conf.SendSingle {
			lines = make([]string, 0, len(dd))
			for _, d := range dd {
				mm, err := m.transformToLine(ctx, d)
				if err != nil {
					return nil, err
				}
				lines = append(lines, mm)
			}
		} else {
			lines, err = m.transformToLines(ctx, dd)
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("influx2 sink needs map or []map, but receive unsupported data %v", dd)
	}
	return lines, nil
}

func (m *influxSink2) transformToLine(ctx api.StreamContext, dd map[string]any) (string, error) {
	v64, err := m.GetTs(dd)
	if err != nil {
		return "", err
	}
	if m.hasTransform {
		jsonBytes, _, err := ctx.TransformOutput(dd)
		if err != nil {
			return "", err
		}
		return string(jsonBytes), nil
	} else {
		od, _, err := transform.TransItem(dd, m.conf.DataField, nil)
		if err != nil {
			return "", fmt.Errorf("fail to select fields %v for data %v", m.conf.Fields, dd)
		}
		d, ok := od.(map[string]any)
		if !ok {
			return "", fmt.Errorf("after fields transformation, result is not a map, got %v", d)
		}
		return m.mapToLine(ctx, d, v64)
	}
}

func (m *influxSink2) mapToLine(ctx api.StreamContext, d map[string]any, tt int64) (string, error) {
	var builder strings.Builder
	builder.WriteString(m.conf.Measurement)

	for k, v := range m.conf.Tags {
		builder.WriteString(",")
		builder.WriteString(k)
		builder.WriteString("=")
		t, err := ctx.ParseTemplate(v, d)
		if err != nil {
			return "", fmt.Errorf("parse %s tag template %s failed, err:%v", k, v, err)
		}
		builder.WriteString(t)
	}
	builder.WriteString(" ")
	c := 0

	if len(m.conf.Fields) > 0 {
		for _, k := range m.conf.Fields {
			c = writeLine(c, &builder, k, d[k])
		}
	} else {
		for k, v := range d {
			c = writeLine(c, &builder, k, v)
		}
	}

	builder.WriteString(" ")
	builder.WriteString(fmt.Sprintf("%v", tt))
	return builder.String(), nil
}

func writeLine(c int, builder *strings.Builder, k string, v any) int {
	if c > 0 {
		builder.WriteString(",")
	}
	c++
	builder.WriteString(k)
	builder.WriteString("=")
	switch value := v.(type) {
	case string:
		builder.WriteString(fmt.Sprintf("\"%s\"", value))
	default:
		builder.WriteString(fmt.Sprintf("%v", value))
	}
	return c
}

func (m *influxSink2) transformToLines(ctx api.StreamContext, dd []map[string]any) ([]string, error) {
	if m.hasTransform {
		jsonBytes, _, err := ctx.TransformOutput(dd)
		if err != nil {
			return nil, err
		}
		return []string{string(jsonBytes)}, nil
	} else {
		// calculate ts first before it is converted
		tts := make([]int64, 0, len(dd))
		for _, d := range dd {
			v64, err := m.GetTs(d)
			if err != nil {
				return nil, err
			}
			tts = append(tts, v64)
		}

		d, _, _ := transform.TransItem(dd, m.conf.DataField, nil)
		ddd, ok := d.([]map[string]any)
		if !ok {
			return nil, fmt.Errorf("after fields transformation, result is not a []map, got %v", d)
		}

		results := make([]string, 0, len(ddd))
		for i, d := range ddd {
			r, err := m.mapToLine(ctx, d, tts[i])
			if err != nil {
				return nil, err
			}
			results = append(results, r)
		}
		return results, nil
	}
}

func (m *influxSink2) GetTs(data map[string]any) (int64, error) {
	if m.conf.TsFieldName != "" {
		return m.getTS(data)
	} else {
		switch m.conf.PrecisionStr {
		case "ms":
			return conf.GetNowInMilli(), nil
		case "s":
			return conf.GetNow().Unix(), nil
		case "us":
			return conf.GetNow().UnixMicro(), nil
		case "ns":
			return conf.GetNow().UnixNano(), nil
		}
		return conf.GetNowInMilli(), nil
	}
}
