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

package influx2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

// c is the configuration for influx2 sink
type c struct {
	// connection
	Addr         string        `json:"addr"`
	Token        string        `json:"token"`
	Org          string        `json:"org"`
	Bucket       string        `json:"bucket"`
	PrecisionStr string        `json:"precision"`
	Precision    time.Duration `json:"-"`
	// http connection
	// tls
	// write options
	UseLineProtocol bool              `json:"useLineProtocol"` // 0: json, 1: line protocol
	Measurement     string            `json:"measurement"`
	Tags            map[string]string `json:"tags"`
	// common options
	DataField  string   `json:"dataField"`
	Fields     []string `json:"fields"`
	BatchSize  int      `json:"batchSize"`
	SendSingle bool     `json:"sendSingle"`
}

// influxSink2 is the sink for influx2.
// To ensure exact order, it uses blocking write api to write data to influxdb2.
type influxSink2 struct {
	conf c
	// tagKey    string
	// tagValue  string
	// already have
	// dataField string
	// fields    []string

	hasTransform bool
	cli          client.Client
}

func (m *influxSink2) Configure(props map[string]any) error {
	m.conf = c{
		PrecisionStr: "ms",
	}
	err := cast.MapToStruct(props, &m.conf)
	if err != nil {
		return fmt.Errorf("error configuring influx2 sink: %s", err)
	}
	if len(m.conf.Addr) == 0 {
		return fmt.Errorf("addr is required")
	}
	if len(m.conf.Org) == 0 {
		return fmt.Errorf("org is required")
	}
	if len(m.conf.Bucket) == 0 {
		return fmt.Errorf("bucket is required")
	}
	switch m.conf.PrecisionStr {
	case "ms":
		m.conf.Precision = time.Millisecond
	case "s":
		m.conf.Precision = time.Second
	case "us":
		m.conf.Precision = time.Microsecond
	case "ns":
		m.conf.Precision = time.Nanosecond
	default:
		return fmt.Errorf("precision %s is not supported", m.conf.PrecisionStr)
	}
	if len(m.conf.Measurement) == 0 {
		return fmt.Errorf("measurement is required")
	}
	return err
}

func (m *influxSink2) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("influx2 sink open")
	if m.conf.BatchSize <= 0 {
		m.conf.BatchSize = 1
	}
	options := client.DefaultOptions().SetPrecision(m.conf.Precision).SetBatchSize(uint(m.conf.BatchSize))
	m.cli = client.NewClientWithOptions(m.conf.Addr, m.conf.Token, options)
	_, err := m.cli.Ping(context.Background())
	return err
}

func (m *influxSink2) Collect(ctx api.StreamContext, data any) error {
	logger := ctx.GetLogger()
	// Write out with blocking API to keep order. Batch is done by sink node side
	writeAPI := m.cli.WriteAPIBlocking(m.conf.Org, m.conf.Bucket)
	if !m.conf.UseLineProtocol {
		pts, err := m.transformPoints(ctx, data)
		if err != nil {
			return err
		}
		err = writeAPI.WritePoint(ctx, pts...)
		if err != nil {
			logger.Errorf("influx2 sink error: %v", err)
			return fmt.Errorf(`%s: influx2 sink fails to send out the data . %v`, errorx.IOErr, err)
		}
	} else {
		lines, err := m.transformLines(ctx, data)
		if err != nil {
			return err
		}
		err = writeAPI.WriteRecord(ctx, lines...)
		if err != nil {
			logger.Errorf("influx2 sink error: %v", err)
			return fmt.Errorf(`%s: influx2 sink fails to send out the data . %v`, errorx.IOErr, err)
		}
	}
	logger.Debug("insert data into influxdb2 success")
	return nil
}

func (m *influxSink2) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("influx2 sink close")
	m.cli.Close()
	return nil
}

func (m *influxSink2) transformPoints(ctx api.StreamContext, dd any) ([]*write.Point, error) {
	var pts []*write.Point
	switch dd := dd.(type) {
	case map[string]any:
		ctx.GetLogger().Debugf("influx2 sink receive data %v", dd)
		mm, err := m.transformToMap(ctx, dd)
		if err != nil {
			return nil, err
		}
		pts = append(pts, client.NewPoint(m.conf.Measurement, m.conf.Tags, mm, conf.GetNow()))
	case []map[string]any:
		ctx.GetLogger().Debugf("influx2 sink receive data %v", dd)
		if m.conf.SendSingle {
			pts = make([]*write.Point, 0, len(dd))
			for _, d := range dd {
				mm, err := m.transformToMap(ctx, d)
				if err != nil {
					return nil, err
				}
				pts = append(pts, client.NewPoint(m.conf.Measurement, m.conf.Tags, mm, conf.GetNow()))
			}
		} else {
			mm, err := m.transformMapsToMap(ctx, dd)
			if err != nil {
				return nil, err
			}
			for _, d := range mm {
				pts = append(pts, client.NewPoint(m.conf.Measurement, m.conf.Tags, d, conf.GetNow()))
			}
		}
	default:
		return nil, fmt.Errorf("influx2 sink needs map or []map, but receive unsupported data %v", dd)
	}
	return pts, nil
}

func (m *influxSink2) transformToMap(ctx api.StreamContext, dd map[string]any) (map[string]any, error) {
	if m.hasTransform {
		jsonBytes, _, err := ctx.TransformOutput(dd)
		if err != nil {
			return nil, err
		}
		m := make(map[string]any)
		err = json.Unmarshal(jsonBytes, &m)
		if err != nil {
			return nil, fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(jsonBytes), err)
		}
		return m, nil
	} else {
		d, _, _ := transform.TransItem(dd, m.conf.DataField, m.conf.Fields)
		if dm, ok := d.(map[string]any); !ok {
			return nil, nil
		} else {
			return dm, nil
		}
	}
}

func (m *influxSink2) transformMapsToMap(ctx api.StreamContext, dds []map[string]any) ([]map[string]any, error) {
	if m.hasTransform {
		jsonBytes, _, err := ctx.TransformOutput(dds)
		if err != nil {
			return nil, err
		}
		// if not json array, try to unmarshal as json object
		m := make(map[string]any)
		err = json.Unmarshal(jsonBytes, &m)
		if err == nil {
			return []map[string]any{m}, nil
		}
		var ms []map[string]any
		err = json.Unmarshal(jsonBytes, &ms)
		if err != nil {
			return nil, fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(jsonBytes), err)
		}
		return ms, nil
	} else {
		d, _, _ := transform.TransItem(dds, m.conf.DataField, m.conf.Fields)
		if md, ok := d.([]map[string]any); !ok {
			return nil, nil
		} else {
			return md, nil
		}
	}
}

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
	if m.hasTransform {
		jsonBytes, _, err := ctx.TransformOutput(dd)
		if err != nil {
			return "", err
		}
		return string(jsonBytes), nil
	} else {
		od, _, err := transform.TransItem(dd, m.conf.DataField, m.conf.Fields)
		if err != nil {
			return "", fmt.Errorf("fail to select fields %v for data %v", m.conf.Fields, dd)
		}
		d, ok := od.(map[string]any)
		if !ok {
			return "", fmt.Errorf("after fields transformation, result is not a map, got %v", d)
		}

		var builder strings.Builder
		builder.WriteString(m.conf.Measurement)

		for k, v := range m.conf.Tags {
			builder.WriteString(",")
			builder.WriteString(k)
			builder.WriteString("=")
			builder.WriteString(v)
		}
		builder.WriteString(" ")
		c := 0
		for k, v := range d {
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

		}
		builder.WriteString(" ")
		builder.WriteString(fmt.Sprintf("%v", conf.GetNowInMilli()))
		return builder.String(), nil
	}
}

func (m *influxSink2) transformToLines(ctx api.StreamContext, dd []map[string]any) ([]string, error) {
	if m.hasTransform {
		jsonBytes, _, err := ctx.TransformOutput(dd)
		if err != nil {
			return nil, err
		}
		return []string{string(jsonBytes)}, nil
	} else {
		d, _, _ := transform.TransItem(dd, m.conf.DataField, m.conf.Fields)
		ddd, ok := d.([]map[string]any)
		if !ok {
			return nil, fmt.Errorf("after fields transformation, result is not a []map, got %v", d)
		}

		results := make([]string, 0, len(ddd))
		for _, d := range ddd {
			var builder strings.Builder
			builder.WriteString(m.conf.Measurement)

			for k, v := range m.conf.Tags {
				builder.WriteString(",")
				builder.WriteString(k)
				builder.WriteString("=")
				builder.WriteString(v)
			}
			builder.WriteString(" ")
			c := 0
			for k, v := range d {
				if c > 0 {
					builder.WriteString(",")
				}
				c++
				builder.WriteString(k)
				builder.WriteString("=")
				builder.WriteString(fmt.Sprintf("%v", v))
			}
			builder.WriteString(" ")
			builder.WriteString(fmt.Sprintf("%d", conf.GetNowInMilli()))
			results = append(results, builder.String())
		}
		return results, nil
	}
}

func GetSink() api.Sink {
	return &influxSink2{}
}
