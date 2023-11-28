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

package influx

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

// c is the configuration for influx2 sink
type c struct {
	// connection
	Addr     string `json:"addr"`
	Username string `json:"username"`
	Password string `json:"password"`
	// http connection
	// tls conf in cert.go
	// write options
	Database     string            `json:"database"`
	PrecisionStr string            `json:"precision"`
	Measurement  string            `json:"measurement"`
	Tags         map[string]string `json:"tags"`
	TsFieldName  string            `json:"tsFieldName"`
	// common options
	DataField    string   `json:"dataField"`
	Fields       []string `json:"fields"`
	BatchSize    int      `json:"batchSize"`
	SendSingle   bool     `json:"sendSingle"`
	DataTemplate string   `json:"dataTemplate"`
}

type influxSink struct {
	conf c
	// internal conf value
	password     string
	hasTransform bool
	// temp variables
	tagEval map[string]string
	bp      client.BatchPoints
	cli     client.Client
}

func (m *influxSink) Configure(props map[string]interface{}) error {
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
	if len(m.conf.Database) == 0 {
		return fmt.Errorf("database is required")
	}
	switch m.conf.PrecisionStr {
	case "ms", "s", "us", "ns":
		// no error
	default:
		return fmt.Errorf("precision %s is not supported", m.conf.PrecisionStr)
	}
	if len(m.conf.Measurement) == 0 {
		return fmt.Errorf("measurement is required")
	}
	tlsConf, err := cert.GenTLSForClientFromProps(props)
	if err != nil {
		return fmt.Errorf("error configuring tls: %s", err)
	}
	if m.conf.BatchSize <= 0 {
		m.conf.BatchSize = 1
	}
	m.password = m.conf.Password
	m.conf.Password = "******"
	insecureSkip := tlsConf.InsecureSkipVerify

	m.cli, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:               m.conf.Addr,
		Username:           m.conf.Username,
		Password:           m.password,
		InsecureSkipVerify: insecureSkip,
		TLSConfig:          tlsConf,
	})
	if err != nil {
		return fmt.Errorf("error creating influx client: %s", err)
	}

	if m.conf.DataTemplate != "" {
		m.hasTransform = true
	}
	m.tagEval = make(map[string]string)
	return err
}

func (m *influxSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("influx sink open with properties %+v", m.conf)
	err := m.parseTemplates(ctx)
	if err != nil {
		return err
	}
	// Test connection. Put it here to avoid server connection when running test in Configure
	_, _, err = m.cli.Ping(time.Second * 10)
	if err != nil {
		return fmt.Errorf("error pinging influx server: %s", err)
	}
	m.bp, err = client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.conf.Database,
		Precision: m.conf.PrecisionStr,
	})
	return err
}

func (m *influxSink) Collect(ctx api.StreamContext, data any) error {
	logger := ctx.GetLogger()
	err := m.transformPoints(ctx, data)
	if err != nil {
		logger.Error(err)
		return err
	}
	// Write the batch
	err = m.cli.Write(m.bp)
	if err != nil {
		logger.Error(err)
		return err
	}
	logger.Debug("influx insert success")
	return nil
}

func (m *influxSink) transformPoints(ctx api.StreamContext, data any) error {
	var err error
	m.bp, err = client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.conf.Database,
		Precision: m.conf.PrecisionStr,
	})
	if err != nil {
		return err
	}

	// Switch data to point
	switch v := data.(type) {
	case map[string]interface{}:
		pt, err := m.singleMapToPoint(ctx, v)
		if err != nil {
			return err
		}
		m.bp.AddPoint(pt)
	case []map[string]interface{}:
		if m.conf.SendSingle {
			for _, d := range v {
				pt, err := m.singleMapToPoint(ctx, d)
				if err != nil {
					return err
				}
				m.bp.AddPoint(pt)
			}
		} else {
			mm, err := m.transformMapsToMap(ctx, v)
			if err != nil {
				return err
			}
			for _, d := range mm {
				tt, err := m.getTime(d)
				if err != nil {
					return err
				}
				pt, err := m.mapToPoint(ctx, d, tt)
				if err != nil {
					return err
				}
				m.bp.AddPoint(pt)
			}
		}
	}
	return nil
}

func (m *influxSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("influx sink close")
	return m.cli.Close()
}

func GetSink() api.Sink {
	return &influxSink{}
}

func (m *influxSink) parseTemplates(ctx api.StreamContext) error {
	for _, v := range m.conf.Tags {
		_, err := ctx.ParseTemplate(v, nil)
		if err != nil && strings.HasPrefix(err.Error(), "Template Invalid") {
			return err
		}
	}
	return nil
}

// Method to convert map to influxdb point, including the sink transforms + map to point
func (m *influxSink) singleMapToPoint(ctx api.StreamContext, dd map[string]any) (*client.Point, error) {
	tt, err := m.getTime(dd)
	if err != nil {
		return nil, err
	}
	mm, err := m.transformToMap(ctx, dd)
	if err != nil {
		return nil, err
	}
	return m.mapToPoint(ctx, mm, tt)
}

// Internal method to get time from map with tsFieldName
func (m *influxSink) getTime(data map[string]any) (time.Time, error) {
	if m.conf.TsFieldName != "" {
		v64, err := m.getTS(data)
		if err != nil {
			return time.Time{}, err
		}
		switch m.conf.PrecisionStr {
		case "ms":
			return time.UnixMilli(v64), nil
		case "s":
			return time.Unix(v64, 0), nil
		case "us":
			return time.UnixMicro(v64), nil
		case "ns":
			return time.Unix(0, v64), nil
		}
		return time.UnixMilli(v64), nil
	} else {
		return conf.GetNow(), nil
	}
}

func (m *influxSink) getTS(data map[string]any) (int64, error) {
	v, ok := data[m.conf.TsFieldName]
	if !ok {
		return 0, fmt.Errorf("time field %s not found", m.conf.TsFieldName)
	}
	v64, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
	if err != nil {
		return 0, fmt.Errorf("time field %s can not convert to timestamp(int64) : %v", m.conf.TsFieldName, v)
	}
	return v64, nil
}

// Method of sink transforms for a single map
func (m *influxSink) transformToMap(ctx api.StreamContext, dd map[string]any) (map[string]any, error) {
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
		d, _, _ := transform.TransItem(dd, m.conf.DataField, nil)
		if dm, ok := d.(map[string]any); !ok {
			return nil, nil
		} else {
			return dm, nil
		}
	}
}

// Internal method to transform map to influxdb point
func (m *influxSink) mapToPoint(ctx api.StreamContext, mm map[string]any, tt time.Time) (*client.Point, error) {
	for k, v := range m.conf.Tags {
		vv, err := ctx.ParseTemplate(v, mm)
		if err != nil {
			return nil, fmt.Errorf("parse %s tag template %s failed, err:%v", k, v, err)
		}
		// convertAll has no error
		vs, _ := cast.ToString(vv, cast.CONVERT_ALL)
		m.tagEval[k] = vs
	}

	return client.NewPoint(m.conf.Measurement, m.tagEval, m.SelectFields(mm), tt)
}

func (m *influxSink) SelectFields(data map[string]any) map[string]any {
	if len(m.conf.Fields) > 0 {
		output := make(map[string]any, len(m.conf.Fields))
		for _, field := range m.conf.Fields {
			output[field] = data[field]
		}
		return output
	} else {
		return data
	}
}

// Internal method of sink transforms for a slice of maps
func (m *influxSink) transformMapsToMap(ctx api.StreamContext, dds []map[string]any) ([]map[string]any, error) {
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
		d, _, _ := transform.TransItem(dds, m.conf.DataField, nil)
		if md, ok := d.([]map[string]any); !ok {
			return nil, nil
		} else {
			return md, nil
		}
	}
}
