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

//go:build edgex
// +build edgex

package edgex

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	v2 "github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos/requests"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"reflect"
)

type SinkConf struct {
	Topic        string      `json:"topic"`
	TopicPrefix  string      `json:"topicPrefix"`
	MessageType  messageType `json:"messageType"`
	ContentType  string      `json:"contentType"`
	DeviceName   string      `json:"deviceName"`
	ProfileName  string      `json:"profileName"`
	SourceName   string      `json:"sourceName"`
	Metadata     string      `json:"metadata"`
	DataTemplate string      `json:"dataTemplate"`
}

type EdgexMsgBusSink struct {
	c *SinkConf

	config map[string]interface{}
	topic  string

	cli api.MessageClient
}

func (ems *EdgexMsgBusSink) Configure(ps map[string]interface{}) error {

	c := &SinkConf{
		MessageType: MessageTypeEvent,
		ContentType: "application/json",
		DeviceName:  "ekuiper",
		ProfileName: "ekuiperProfile",
	}

	err := cast.MapToStruct(ps, c)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", ps, err)
	}

	if c.MessageType != MessageTypeEvent && c.MessageType != MessageTypeRequest {
		return fmt.Errorf("specified wrong messageType value %s", c.MessageType)
	}

	if c.MessageType == MessageTypeEvent && c.ContentType != "application/json" {
		return fmt.Errorf("specified wrong contentType value %s: only 'application/json' is supported if messageType is event", c.ContentType)
	}

	if c.Topic != "" && c.TopicPrefix != "" {
		return fmt.Errorf("not allow to specify both topic and topicPrefix, please set one only")
	}
	ems.c = c
	ems.config = ps

	return nil
}

func (ems *EdgexMsgBusSink) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()

	cli, err := clients.GetClient("edgex", ems.config)
	if err != nil {
		log.Errorf("found error when get edgex client, error %s", err.Error())
		return err
	}

	ems.cli = cli

	if ems.c.SourceName == "" {
		ems.c.SourceName = ctx.GetRuleId()
	}

	if ems.c.Topic == "" && ems.c.TopicPrefix == "" {
		ems.topic = "application"
	} else if ems.c.Topic != "" {
		ems.topic = ems.c.Topic
	} else if ems.c.Metadata == "" { // If meta data are static, the "dynamic" topic is static
		ems.topic = fmt.Sprintf("%s/%s/%s/%s", ems.c.TopicPrefix, ems.c.ProfileName, ems.c.DeviceName, ems.c.SourceName)
	} else {
		ems.topic = "" // calculate dynamically
	}
	return nil
}

func (ems *EdgexMsgBusSink) produceEvents(ctx api.StreamContext, item interface{}) (*dtos.Event, error) {
	if ems.c.DataTemplate != "" {
		jsonBytes, _, err := ctx.TransformOutput(item)
		if err != nil {
			return nil, err
		}
		tm := make(map[string]interface{})
		err = json.Unmarshal(jsonBytes, &tm)
		if err != nil {
			return nil, fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(jsonBytes), err)
		}
		item = tm
	}
	var m []map[string]interface{}
	switch payload := item.(type) {
	case map[string]interface{}:
		m = []map[string]interface{}{payload}
	case []map[string]interface{}:
		m = payload
	default:
		// impossible
		return nil, fmt.Errorf("receive invalid data %v", item)
	}
	m1 := ems.getMeta(m)
	event := m1.createEvent()
	//Override the devicename if user specified the value
	if event.DeviceName == "" {
		event.DeviceName = ems.c.DeviceName
	}
	if event.ProfileName == "" {
		event.ProfileName = ems.c.ProfileName
	}
	if event.SourceName == "" {
		event.SourceName = ems.c.SourceName
	}
	for _, v := range m {
		for k1, v1 := range v {
			// Ignore nil values
			if k1 == ems.c.Metadata || v1 == nil {
				continue
			} else {
				var (
					vt  string
					vv  interface{}
					err error
				)
				mm1 := m1.readingMeta(ctx, k1)
				if mm1 != nil && mm1.valueType != nil {
					vt = *mm1.valueType
					vv, err = getValueByType(v1, vt)
				} else {
					vt, vv, err = getValueType(v1)
				}
				if err != nil {
					ctx.GetLogger().Errorf("%v", err)
					continue
				}
				switch vt {
				case v2.ValueTypeBinary:
					// default media type
					event.AddBinaryReading(k1, vv.([]byte), "application/text")
				case v2.ValueTypeObject:
					event.AddObjectReading(k1, vv)
				default:
					err = event.AddSimpleReading(k1, vt, vv)
				}

				if err != nil {
					ctx.GetLogger().Errorf("%v", err)
					continue
				}
				r := event.Readings[len(event.Readings)-1]
				if mm1 != nil {
					event.Readings[len(event.Readings)-1] = mm1.decorate(&r)
				}
			}
		}
	}
	return event, nil
}

func getValueType(v interface{}) (string, interface{}, error) {
	vt := reflect.TypeOf(v)
	if vt == nil {
		return "", nil, fmt.Errorf("unsupported value nil")
	}
	k := vt.Kind()
	switch k {
	case reflect.Bool:
		return v2.ValueTypeBool, v, nil
	case reflect.String:
		return v2.ValueTypeString, v, nil
	case reflect.Uint8:
		return v2.ValueTypeUint8, v, nil
	case reflect.Uint16:
		return v2.ValueTypeUint16, v, nil
	case reflect.Uint32:
		return v2.ValueTypeUint32, v, nil
	case reflect.Uint64:
		return v2.ValueTypeUint64, v, nil
	case reflect.Uint:
		return v2.ValueTypeUint64, uint64(v.(uint)), nil
	case reflect.Int8:
		return v2.ValueTypeInt8, v, nil
	case reflect.Int16:
		return v2.ValueTypeInt16, v, nil
	case reflect.Int32:
		return v2.ValueTypeInt32, v, nil
	case reflect.Int64:
		return v2.ValueTypeInt64, v, nil
	case reflect.Int:
		return v2.ValueTypeInt64, int64(v.(int)), nil
	case reflect.Float32:
		return v2.ValueTypeFloat32, v, nil
	case reflect.Float64:
		return v2.ValueTypeFloat64, v, nil
	case reflect.Slice:
		switch arrayValue := v.(type) {
		case []interface{}:
			if len(arrayValue) > 0 {
				kt := reflect.TypeOf(arrayValue[0])
				if kt == nil {
					return "", nil, fmt.Errorf("unsupported value %v(%s), the first element is nil", v, k)
				}
				switch kt.Kind() {
				case reflect.Bool:
					result := make([]bool, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(bool)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []bool for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeBoolArray, result, nil
				case reflect.String:
					result := make([]string, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(string)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []string for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeStringArray, result, nil
				case reflect.Int8:
					result := make([]int8, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(int8)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []int8 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeInt8Array, result, nil
				case reflect.Int16:
					result := make([]int16, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(int16)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []int16 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeInt16Array, result, nil
				case reflect.Int32:
					result := make([]int32, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(int32)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []int32 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeInt32Array, result, nil
				case reflect.Int64, reflect.Int:
					result := make([]int64, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(int64)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []int64 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeInt64Array, result, nil
				case reflect.Uint8:
					result := make([]uint8, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(uint8)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []uint8 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeUint8Array, result, nil
				case reflect.Uint16:
					result := make([]uint16, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(uint16)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []uint16 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeUint16Array, result, nil
				case reflect.Uint32:
					result := make([]uint32, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(uint32)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []uint32 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeUint32Array, result, nil
				case reflect.Uint64, reflect.Uint:
					result := make([]uint64, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(uint64)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []uint64 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeUint64Array, result, nil
				case reflect.Float32:
					result := make([]float32, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(float32)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []float32 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeFloat64Array, result, nil
				case reflect.Float64:
					result := make([]float64, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(float64)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []float64 for %v", v)
						}
						result[i] = temp
					}
					return v2.ValueTypeFloat64Array, result, nil
				}
			} else { // default to string array
				return v2.ValueTypeStringArray, []string{}, nil
			}
		case []byte:
			return v2.ValueTypeBinary, v, nil
		}
	}
	return v2.ValueTypeObject, v, nil
}

func getValueByType(v interface{}, vt string) (interface{}, error) {
	switch vt {
	case v2.ValueTypeBool:
		return cast.ToBool(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeInt8:
		return cast.ToInt8(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeInt16:
		return cast.ToInt16(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeInt32:
		return cast.ToInt32(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeInt64:
		return cast.ToInt64(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeUint8:
		return cast.ToUint8(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeUint16:
		return cast.ToUint16(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeUint32:
		return cast.ToUint32(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeUint64:
		return cast.ToUint64(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeFloat32:
		return cast.ToFloat32(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeFloat64:
		return cast.ToFloat64(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeString:
		return cast.ToString(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeBoolArray:
		return cast.ToBoolSlice(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeInt8Array:
		return cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
			return cast.ToInt8(input, sn)
		}, "int8", cast.CONVERT_SAMEKIND)
	case v2.ValueTypeInt16Array:
		return cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
			return cast.ToInt16(input, sn)
		}, "int16", cast.CONVERT_SAMEKIND)
	case v2.ValueTypeInt32Array:
		return cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
			return cast.ToInt32(input, sn)
		}, "int32", cast.CONVERT_SAMEKIND)
	case v2.ValueTypeInt64Array:
		return cast.ToInt64Slice(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeUint8Array:
		return cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
			return cast.ToUint8(input, sn)
		}, "uint8", cast.CONVERT_SAMEKIND)
	case v2.ValueTypeUint16Array:
		return cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
			return cast.ToUint16(input, sn)
		}, "uint16", cast.CONVERT_SAMEKIND)
	case v2.ValueTypeUint32Array:
		return cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
			return cast.ToUint32(input, sn)
		}, "uint32", cast.CONVERT_SAMEKIND)
	case v2.ValueTypeUint64Array:
		return cast.ToUint64Slice(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeFloat32Array:
		return cast.ToTypedSlice(v, func(input interface{}, sn cast.Strictness) (interface{}, error) {
			return cast.ToFloat32(input, sn)
		}, "float32", cast.CONVERT_SAMEKIND)
	case v2.ValueTypeFloat64Array:
		return cast.ToFloat64Slice(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeStringArray:
		return cast.ToStringSlice(v, cast.CONVERT_SAMEKIND)
	case v2.ValueTypeBinary:
		var (
			bv  []byte
			err error
		)
		switch vv := v.(type) {
		case string:
			if bv, err = base64.StdEncoding.DecodeString(vv); err != nil {
				return nil, fmt.Errorf("fail to decode binary value from %s: %v", vv, err)
			}
		case []byte:
			bv = vv
		default:
			return nil, fmt.Errorf("fail to decode binary value from %v: not binary type", vv)
		}
		return bv, nil
	case v2.ValueTypeObject:
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported type %v", vt)
	}
}

func (ems *EdgexMsgBusSink) getMeta(result []map[string]interface{}) *meta {
	if ems.c.Metadata == "" {
		return newMetaFromMap(nil)
	}
	//Try to get the meta field
	for _, v := range result {
		if m, ok := v[ems.c.Metadata]; ok {
			if m1, ok1 := m.(map[string]interface{}); ok1 {
				return newMetaFromMap(m1)
			} else {
				conf.Log.Infof("Specified a meta field, but the field does not contains any EdgeX metadata.")
			}
		}
	}
	return newMetaFromMap(nil)
}

func (ems *EdgexMsgBusSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	evt, err := ems.produceEvents(ctx, item)
	if err != nil {
		return fmt.Errorf("Failed to convert to EdgeX event: %s.", err.Error())
	}
	var (
		data  []byte
		topic string
	)
	if ems.c.MessageType == MessageTypeRequest {
		req := requests.NewAddEventRequest(*evt)
		data, _, err = req.Encode()
		if err != nil {
			return fmt.Errorf("unexpected error encode event %v", err)
		}
	} else {
		data, err = json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("unexpected error MarshalEvent %v", err)
		}
	}

	if ems.topic == "" { // dynamic topic
		topic = fmt.Sprintf("%s/%s/%s/%s", ems.c.TopicPrefix, evt.ProfileName, evt.DeviceName, evt.SourceName)
	} else {
		topic = ems.topic
	}

	para := map[string]interface{}{
		"contentType": ems.c.ContentType,
	}
	if e := ems.cli.Publish(ctx, topic, data, para); e != nil {
		logger.Errorf("%s: found error %s when publish to EdgeX message bus.\n", e.Error(), e.Error())
		return fmt.Errorf("%s:%s", errorx.IOErr, e.Error())
	}
	logger.Debugf("Published %+v to EdgeX message bus topic %s", evt, topic)

	return nil
}

func (ems *EdgexMsgBusSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing edgex sink")
	if ems.cli != nil {
		clients.ReleaseClient(ctx, ems.cli)
	}
	return nil
}

type eventMeta struct {
	id          *string
	deviceName  string
	profileName string
	sourceName  string
	origin      *int64
	tags        map[string]interface{}
}

type readingMeta struct {
	id           *string
	deviceName   *string
	profileName  *string
	resourceName *string
	origin       *int64
	valueType    *string
	mediaType    *string
}

func (m *readingMeta) decorate(r *dtos.BaseReading) dtos.BaseReading {
	if m.id != nil {
		r.Id = *m.id
	}
	if m.deviceName != nil {
		r.DeviceName = *m.deviceName
	}
	if m.profileName != nil {
		r.ProfileName = *m.profileName
	}
	if m.origin != nil {
		r.Origin = *m.origin
	}
	if m.valueType != nil {
		r.ValueType = *m.valueType
	}
	if m.mediaType != nil {
		r.MediaType = *m.mediaType
	}
	return *r
}

type meta struct {
	eventMeta
	readingMetas map[string]interface{}
}

func newMetaFromMap(m1 map[string]interface{}) *meta {
	result := &meta{
		eventMeta: eventMeta{},
	}
	for k, v := range m1 {
		switch k {
		case "id":
			if v1, ok := v.(string); ok {
				result.id = &v1
			}
		case "deviceName":
			if v1, ok := v.(string); ok {
				result.deviceName = v1
			}
		case "profileName":
			if v1, ok := v.(string); ok {
				result.profileName = v1
			}
		case "sourceName":
			if v1, ok := v.(string); ok {
				result.sourceName = v1
			}
		case "origin":
			if v1, ok := v.(float64); ok {
				temp := int64(v1)
				result.origin = &temp
			}
		case "tags":
			if v1, ok1 := v.(map[string]interface{}); ok1 {
				result.tags = v1
			}
		default:
			if result.readingMetas == nil {
				result.readingMetas = make(map[string]interface{})
			}
			result.readingMetas[k] = v
		}
	}
	return result
}

func (m *meta) readingMeta(ctx api.StreamContext, readingName string) *readingMeta {
	vi, ok := m.readingMetas[readingName]
	if !ok {
		return nil
	}
	m1, ok := vi.(map[string]interface{})
	if !ok {
		ctx.GetLogger().Errorf("reading %s meta is not a map, but %v", readingName, vi)
		return nil
	}
	result := &readingMeta{}
	for k, v := range m1 {
		switch k {
		case "id":
			if v1, ok := v.(string); ok {
				result.id = &v1
			}
		case "deviceName":
			if v1, ok := v.(string); ok {
				result.deviceName = &v1
			}
		case "profileName":
			if v1, ok := v.(string); ok {
				result.profileName = &v1
			}
		case "resourceName":
			if v1, ok := v.(string); ok {
				result.resourceName = &v1
			}
		case "origin":
			if v1, ok := v.(float64); ok {
				temp := int64(v1)
				result.origin = &temp
			}
		case "valueType":
			if v1, ok := v.(string); ok {
				result.valueType = &v1
			}
		case "mediaType":
			if v1, ok := v.(string); ok {
				result.mediaType = &v1
			}
		default:
			ctx.GetLogger().Warnf("reading %s meta got unknown field %s of value %v", readingName, k, v)
		}
	}
	return result
}

func (m *meta) createEvent() *dtos.Event {
	event := dtos.NewEvent(m.profileName, m.deviceName, m.sourceName)
	if m.id != nil {
		event.Id = *m.id
	}
	if m.origin != nil {
		event.Origin = *m.origin
	}
	if m.tags != nil {
		event.Tags = m.tags
	}
	return &event
}
