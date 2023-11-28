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

package tspoint

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type WriteOptions struct {
	PrecisionStr string `json:"precision"`

	Tags        map[string]string `json:"tags"`
	TsFieldName string            `json:"tsFieldName"`
	// common options
	DataField    string   `json:"dataField"`
	Fields       []string `json:"fields"`
	SendSingle   bool     `json:"sendSingle"`
	DataTemplate string   `json:"dataTemplate"`
}

func (o *WriteOptions) Validate() error {
	switch o.PrecisionStr {
	case "ms", "s", "us", "ns":
		// no error
	default:
		return fmt.Errorf("precision %s is not supported", o.PrecisionStr)
	}
	return nil
}

func (o *WriteOptions) ValidateTagTemplates(ctx api.StreamContext) error {
	for _, v := range o.Tags {
		_, err := ctx.ParseTemplate(v, nil)
		if err != nil && strings.HasPrefix(err.Error(), "Template Invalid") {
			return err
		}
	}
	return nil
}

type RawPoint struct {
	Fields map[string]any
	Tags   map[string]string
	Tt     time.Time
	Ts     int64
}

func SinkTransform(ctx api.StreamContext, data any, options *WriteOptions) ([]*RawPoint, error) {
	var pts []*RawPoint
	switch dd := data.(type) {
	case map[string]any:
		pt, err := singleMapToPoint(ctx, dd, options)
		if err != nil {
			return nil, err
		}
		pts = append(pts, pt)
	case []map[string]any:
		if options.SendSingle {
			pts = make([]*RawPoint, 0, len(dd))
			for _, d := range dd {
				pt, err := singleMapToPoint(ctx, d, options)
				if err != nil {
					return nil, err
				}
				pts = append(pts, pt)
			}
		} else {
			mm, err := transformMapsToMap(ctx, dd, options)
			if err != nil {
				return nil, err
			}
			pts = make([]*RawPoint, 0, len(mm))
			// TODO possible problem here that the ts filed is transformed out
			for _, d := range mm {
				tt, ts, err := getTime(d, options.TsFieldName, options.PrecisionStr)
				if err != nil {
					return nil, err
				}
				pt, err := mapToPoint(ctx, d, options, tt, ts)
				if err != nil {
					return nil, err
				}
				pts = append(pts, pt)
			}
		}
	default:
		return nil, fmt.Errorf("sink needs map or []map, but receive unsupported data %v", dd)
	}
	return pts, nil
}

// TransformRaw if the result format is raw format and dataTemplate is set, use this.
func TransformRaw(ctx api.StreamContext, data any, options *WriteOptions) ([][]byte, error) {
	var raws [][]byte
	switch dd := data.(type) {
	case map[string]any:
		formatBytes, _, err := ctx.TransformOutput(dd)
		if err != nil {
			return nil, err
		}
		raws = append(raws, formatBytes)
	case []map[string]any:
		if options.SendSingle {
			raws = make([][]byte, 0, len(dd))
			for _, d := range dd {
				formatBytes, _, err := ctx.TransformOutput(d)
				if err != nil {
					return nil, err
				}
				raws = append(raws, formatBytes)
			}
		} else {
			formatBytes, _, err := ctx.TransformOutput(dd)
			if err != nil {
				return nil, err
			}
			raws = append(raws, formatBytes)
		}
	default:
		return nil, fmt.Errorf("sink needs map or []map, but receive unsupported data %v", dd)
	}
	return raws, nil
}

// Method to convert map to influxdb point, including the sink transforms + map to point
func singleMapToPoint(ctx api.StreamContext, dd map[string]any, options *WriteOptions) (*RawPoint, error) {
	tt, ts, err := getTime(dd, options.TsFieldName, options.PrecisionStr)
	if err != nil {
		return nil, err
	}
	mm, err := transformToMap(ctx, dd, options)
	if err != nil {
		return nil, err
	}
	return mapToPoint(ctx, mm, options, tt, ts)
}

// Internal method to transform map to influxdb point
func mapToPoint(ctx api.StreamContext, mm map[string]any, options *WriteOptions, tt time.Time, ts int64) (*RawPoint, error) {
	tagEval := make(map[string]string, len(options.Tags))
	for k, v := range options.Tags {
		vv, err := ctx.ParseTemplate(v, mm)
		if err != nil {
			return nil, fmt.Errorf("parse %s tag template %s failed, err:%v", k, v, err)
		}
		// convertAll has no error
		vs, _ := cast.ToString(vv, cast.CONVERT_ALL)
		tagEval[k] = vs
	}
	return &RawPoint{
		Fields: selectFields(mm, options),
		Tags:   tagEval,
		Tt:     tt,
		Ts:     ts,
	}, nil
}

func transformToMap(ctx api.StreamContext, dd map[string]any, options *WriteOptions) (map[string]any, error) {
	if options.DataTemplate != "" {
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
		d, _, _ := transform.TransItem(dd, options.DataField, nil)
		if dm, ok := d.(map[string]any); !ok {
			return nil, nil
		} else {
			return dm, nil
		}
	}
}

// Internal method of sink transforms for a slice of maps
func transformMapsToMap(ctx api.StreamContext, dds []map[string]any, options *WriteOptions) ([]map[string]any, error) {
	if options.DataTemplate != "" {
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
		d, _, _ := transform.TransItem(dds, options.DataField, nil)
		if md, ok := d.([]map[string]any); !ok {
			return nil, nil
		} else {
			return md, nil
		}
	}
}

// Internal method to get time from map with tsFieldName
func getTime(data map[string]any, tsFieldName string, precisionStr string) (time.Time, int64, error) {
	if tsFieldName != "" {
		v64, err := getTS(data, tsFieldName)
		if err != nil {
			return time.Time{}, v64, err
		}
		switch precisionStr {
		case "ms":
			return time.UnixMilli(v64), v64, nil
		case "s":
			return time.Unix(v64, 0), v64, nil
		case "us":
			return time.UnixMicro(v64), v64, nil
		case "ns":
			return time.Unix(0, v64), v64, nil
		}
		return time.UnixMilli(v64), v64, nil
	} else {
		tt := conf.GetNow()
		switch precisionStr {
		case "ms":
			return tt, tt.UnixMilli(), nil
		case "s":
			return tt, tt.Unix(), nil
		case "us":
			return tt, tt.UnixMicro(), nil
		case "ns":
			return tt, tt.UnixNano(), nil
		}
		return tt, tt.UnixMilli(), nil
	}
}

func getTS(data map[string]any, tsFieldName string) (int64, error) {
	v, ok := data[tsFieldName]
	if !ok {
		return 0, fmt.Errorf("time field %s not found", tsFieldName)
	}
	v64, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
	if err != nil {
		return 0, fmt.Errorf("time field %s can not convert to timestamp(int64) : %v", tsFieldName, v)
	}
	return v64, nil
}

func selectFields(data map[string]any, options *WriteOptions) map[string]any {
	if len(options.Fields) > 0 {
		output := make(map[string]any, len(options.Fields))
		for _, field := range options.Fields {
			output[field] = data[field]
		}
		return output
	} else {
		return data
	}
}
