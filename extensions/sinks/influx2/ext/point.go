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

// Methods to transform data to influxdb points

package influx2

import (
	"encoding/json"
	"fmt"
	"time"

	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

// Entry method for transforming data to influxdb points
func (m *influxSink2) transformPoints(ctx api.StreamContext, dd any) ([]*write.Point, error) {
	var pts []*write.Point
	switch dd := dd.(type) {
	case map[string]any:
		pts = make([]*write.Point, 0, 1)
		ctx.GetLogger().Debugf("influx2 sink receive data %v", dd)
		err := m.singleMapToPoint(ctx, &pts, dd)
		if err != nil {
			return nil, err
		}
	case []map[string]any:
		ctx.GetLogger().Debugf("influx2 sink receive data %v", dd)
		if m.conf.SendSingle {
			pts = make([]*write.Point, 0, len(dd))
			for _, d := range dd {
				err := m.singleMapToPoint(ctx, &pts, d)
				if err != nil {
					return nil, err
				}
			}
		} else {
			mm, err := m.transformMapsToMap(ctx, dd)
			if err != nil {
				return nil, err
			}
			pts = make([]*write.Point, 0, len(mm))
			// TODO possible problem here that the ts filed is transformed out
			for _, d := range mm {
				tt, err := m.getTime(d)
				if err != nil {
					return nil, err
				}
				err = m.mapToPoint(ctx, &pts, d, tt)
				if err != nil {
					return nil, err
				}
			}
		}
	default:
		return nil, fmt.Errorf("influx2 sink needs map or []map, but receive unsupported data %v", dd)
	}
	return pts, nil
}

// Method to convert map to influxdb point, including the sink transforms + map to point
func (m *influxSink2) singleMapToPoint(ctx api.StreamContext, pts *[]*write.Point, dd map[string]any) error {
	tt, err := m.getTime(dd)
	if err != nil {
		return err
	}
	mm, err := m.transformToMap(ctx, dd)
	if err != nil {
		return err
	}
	return m.mapToPoint(ctx, pts, mm, tt)
}

// Method of sink transforms for a single map
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
		d, _, _ := transform.TransItem(dd, m.conf.DataField, nil)
		if dm, ok := d.(map[string]any); !ok {
			return nil, nil
		} else {
			return dm, nil
		}
	}
}

// Internal method to transform map to influxdb point
func (m *influxSink2) mapToPoint(ctx api.StreamContext, pts *[]*write.Point, mm map[string]any, tt time.Time) error {
	for k, v := range m.conf.Tags {
		vv, err := ctx.ParseTemplate(v, mm)
		if err != nil {
			return fmt.Errorf("parse %s tag template %s failed, err:%v", k, v, err)
		}
		// convertAll has no error
		vs, _ := cast.ToString(vv, cast.CONVERT_ALL)
		m.tagEval[k] = vs
	}

	*pts = append(*pts, client.NewPoint(m.conf.Measurement, m.tagEval, m.SelectFields(mm), tt))
	return nil
}

// Internal method of sink transforms for a slice of maps
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
		d, _, _ := transform.TransItem(dds, m.conf.DataField, nil)
		if md, ok := d.([]map[string]any); !ok {
			return nil, nil
		} else {
			return md, nil
		}
	}
}

// Internal method to get time from map with tsFieldName
func (m *influxSink2) getTime(data map[string]any) (time.Time, error) {
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
