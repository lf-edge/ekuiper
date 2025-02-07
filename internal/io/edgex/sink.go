// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package edgex

import (
	"encoding/base64"
	"fmt"
	"reflect"

	v4 "github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/requests"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/edgex/client"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
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
	Fields       []string    `json:"fields"`
	DataField    string      `json:"dataField"`
}

type EdgexMsgBusSink struct {
	c *SinkConf

	config map[string]any
	topic  string

	id         string
	cw         *connection.ConnWrapper
	cli        *client.Client
	sendParams map[string]any
}

func (ems *EdgexMsgBusSink) Provision(ctx api.StreamContext, ps map[string]any) error {
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
	ems.sendParams = map[string]any{
		"contentType": ems.c.ContentType,
	}
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

func (ems *EdgexMsgBusSink) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to edgex server")
	var err error
	ems.id = fmt.Sprintf("%s-%s-%d-edgex-sink", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	ems.cw, err = connection.FetchConnection(ctx, ems.id, "edgex", ems.config, sc)
	if err != nil {
		return err
	}
	conn, err := ems.cw.Wait(ctx)
	if conn == nil {
		return fmt.Errorf("edgex client not ready: %v", err)
	}
	c, ok := conn.(*client.Client)
	if !ok {
		return fmt.Errorf("connection %s should be edgex connection", ems.cw.ID)
	}
	ems.cli = c
	return err
}

func (ems *EdgexMsgBusSink) produceEvents(ctx api.StreamContext, item any) (*dtos.Event, error) {
	var m []map[string]any
	switch payload := item.(type) {
	case map[string]any:
		m = []map[string]any{payload}
	case []map[string]any:
		m = payload
	default:
		// impossible
		return nil, fmt.Errorf("receive invalid data %v", item)
	}
	m1 := ems.getMeta(m)
	event := m1.createEvent()
	// Override the devicename if user specified the value
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
					vv  any
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
				case v4.ValueTypeBinary:
					// default media type
					event.AddBinaryReading(k1, vv.([]byte), "application/text")
				case v4.ValueTypeObject:
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

func getValueType(v any) (string, any, error) {
	vt := reflect.TypeOf(v)
	if vt == nil {
		return "", nil, fmt.Errorf("unsupported value nil")
	}
	k := vt.Kind()
	switch k {
	case reflect.Bool:
		return v4.ValueTypeBool, v, nil
	case reflect.String:
		return v4.ValueTypeString, v, nil
	case reflect.Uint8:
		return v4.ValueTypeUint8, v, nil
	case reflect.Uint16:
		return v4.ValueTypeUint16, v, nil
	case reflect.Uint32:
		return v4.ValueTypeUint32, v, nil
	case reflect.Uint64:
		return v4.ValueTypeUint64, v, nil
	case reflect.Uint:
		return v4.ValueTypeUint64, uint64(v.(uint)), nil
	case reflect.Int8:
		return v4.ValueTypeInt8, v, nil
	case reflect.Int16:
		return v4.ValueTypeInt16, v, nil
	case reflect.Int32:
		return v4.ValueTypeInt32, v, nil
	case reflect.Int64:
		return v4.ValueTypeInt64, v, nil
	case reflect.Int:
		return v4.ValueTypeInt64, int64(v.(int)), nil
	case reflect.Float32:
		return v4.ValueTypeFloat32, v, nil
	case reflect.Float64:
		return v4.ValueTypeFloat64, v, nil
	case reflect.Slice:
		switch arrayValue := v.(type) {
		case []any:
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
					return v4.ValueTypeBoolArray, result, nil
				case reflect.String:
					result := make([]string, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(string)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []string for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeStringArray, result, nil
				case reflect.Int8:
					result := make([]int8, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(int8)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []int8 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeInt8Array, result, nil
				case reflect.Int16:
					result := make([]int16, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(int16)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []int16 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeInt16Array, result, nil
				case reflect.Int32:
					result := make([]int32, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(int32)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []int32 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeInt32Array, result, nil
				case reflect.Int64, reflect.Int:
					result := make([]int64, len(arrayValue))
					for i, av := range arrayValue {
						temp, err := cast.ToInt64(av, cast.CONVERT_SAMEKIND)
						if err != nil {
							return "", nil, fmt.Errorf("unable to cast value to []int64 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeInt64Array, result, nil
				case reflect.Uint8:
					result := make([]uint8, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(uint8)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []uint8 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeUint8Array, result, nil
				case reflect.Uint16:
					result := make([]uint16, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(uint16)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []uint16 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeUint16Array, result, nil
				case reflect.Uint32:
					result := make([]uint32, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(uint32)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []uint32 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeUint32Array, result, nil
				case reflect.Uint64, reflect.Uint:
					result := make([]uint64, len(arrayValue))
					for i, av := range arrayValue {
						temp, err := cast.ToUint64(av, cast.CONVERT_SAMEKIND)
						if err != nil {
							return "", nil, fmt.Errorf("unable to cast value to []uint64 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeUint64Array, result, nil
				case reflect.Float32:
					result := make([]float32, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(float32)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []float32 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeFloat32Array, result, nil
				case reflect.Float64:
					result := make([]float64, len(arrayValue))
					for i, av := range arrayValue {
						temp, ok := av.(float64)
						if !ok {
							return "", nil, fmt.Errorf("unable to cast value to []float64 for %v", v)
						}
						result[i] = temp
					}
					return v4.ValueTypeFloat64Array, result, nil
				}
			} else { // default to string array
				return v4.ValueTypeStringArray, []string{}, nil
			}
		case []byte:
			return v4.ValueTypeBinary, v, nil
		}
	}
	return v4.ValueTypeObject, v, nil
}

func getValueByType(v any, vt string) (any, error) {
	switch vt {
	case v4.ValueTypeBool:
		return cast.ToBool(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeInt8:
		return cast.ToInt8(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeInt16:
		return cast.ToInt16(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeInt32:
		return cast.ToInt32(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeInt64:
		return cast.ToInt64(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeUint8:
		return cast.ToUint8(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeUint16:
		return cast.ToUint16(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeUint32:
		return cast.ToUint32(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeUint64:
		return cast.ToUint64(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeFloat32:
		return cast.ToFloat32(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeFloat64:
		return cast.ToFloat64(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeString:
		return cast.ToString(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeBoolArray:
		return cast.ToBoolSlice(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeInt8Array:
		return cast.ToTypedSlice(v, func(input any, sn cast.Strictness) (any, error) {
			return cast.ToInt8(input, sn)
		}, "int8", cast.CONVERT_SAMEKIND)
	case v4.ValueTypeInt16Array:
		return cast.ToTypedSlice(v, func(input any, sn cast.Strictness) (any, error) {
			return cast.ToInt16(input, sn)
		}, "int16", cast.CONVERT_SAMEKIND)
	case v4.ValueTypeInt32Array:
		return cast.ToTypedSlice(v, func(input any, sn cast.Strictness) (any, error) {
			return cast.ToInt32(input, sn)
		}, "int32", cast.CONVERT_SAMEKIND)
	case v4.ValueTypeInt64Array:
		return cast.ToInt64Slice(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeUint8Array:
		return cast.ToTypedSlice(v, func(input any, sn cast.Strictness) (any, error) {
			return cast.ToUint8(input, sn)
		}, "uint8", cast.CONVERT_SAMEKIND)
	case v4.ValueTypeUint16Array:
		return cast.ToTypedSlice(v, func(input any, sn cast.Strictness) (any, error) {
			return cast.ToUint16(input, sn)
		}, "uint16", cast.CONVERT_SAMEKIND)
	case v4.ValueTypeUint32Array:
		return cast.ToTypedSlice(v, func(input any, sn cast.Strictness) (any, error) {
			return cast.ToUint32(input, sn)
		}, "uint32", cast.CONVERT_SAMEKIND)
	case v4.ValueTypeUint64Array:
		return cast.ToUint64Slice(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeFloat32Array:
		return cast.ToTypedSlice(v, func(input any, sn cast.Strictness) (any, error) {
			return cast.ToFloat32(input, sn)
		}, "float32", cast.CONVERT_SAMEKIND)
	case v4.ValueTypeFloat64Array:
		return cast.ToFloat64Slice(v, cast.CONVERT_SAMEKIND, cast.FORCE_CONVERT)
	case v4.ValueTypeStringArray:
		return cast.ToStringSlice(v, cast.CONVERT_SAMEKIND)
	case v4.ValueTypeBinary:
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
	case v4.ValueTypeObject:
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported type %v", vt)
	}
}

func (ems *EdgexMsgBusSink) getMeta(result []map[string]any) *meta {
	if ems.c.Metadata == "" {
		return newMetaFromMap(nil)
	}
	// Try to get the meta field
	for _, v := range result {
		if m, ok := v[ems.c.Metadata]; ok {
			if m1, ok1 := m.(map[string]any); ok1 {
				return newMetaFromMap(m1)
			} else {
				conf.Log.Infof("Specified a meta field, but the field does not contains any EdgeX metadata.")
			}
		}
	}
	return newMetaFromMap(nil)
}

func (ems *EdgexMsgBusSink) Collect(ctx api.StreamContext, data api.MessageTuple) error {
	return ems.doCollect(ctx, data.ToMap())
}

func (ems *EdgexMsgBusSink) CollectList(ctx api.StreamContext, data api.MessageTupleList) error {
	return ems.doCollect(ctx, data.ToMaps())
}

func (ems *EdgexMsgBusSink) doCollect(ctx api.StreamContext, item any) error {
	evt, err := ems.produceEvents(ctx, item)
	if err != nil {
		return fmt.Errorf("Failed to convert to EdgeX event: %s.", err.Error())
	}
	var (
		req   any
		topic string
	)
	if ems.c.MessageType == MessageTypeRequest {
		req = requests.NewAddEventRequest(*evt)
	} else {
		req = *evt
	}

	if ems.topic == "" { // dynamic topic
		topic = fmt.Sprintf("%s/%s/%s/%s", ems.c.TopicPrefix, evt.ProfileName, evt.DeviceName, evt.SourceName)
	} else {
		topic = ems.topic
	}

	env := types.NewMessageEnvelope(req, ctx)
	env.ContentType = "application/json"
	if pk, ok := ems.sendParams["contentType"]; ok {
		if v, ok := pk.(string); ok {
			env.ContentType = v
		}
	}
	e := ems.cli.Publish(env, topic)
	if e != nil {
		ctx.GetLogger().Errorf("%s: found error %s when publish to EdgeX message bus.\n", e.Error(), e.Error())
		return errorx.NewIOErr(e.Error())
	}
	ctx.GetLogger().Debugf("Published %+v to EdgeX message bus topic %s", evt, topic)
	return nil
}

func (ems *EdgexMsgBusSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing edgex sink")
	if ems.cli != nil {
		_ = ems.cli.Disconnect()
	}
	if ems.cw != nil {
		return connection.DetachConnection(ctx, ems.cw.ID)
	}
	return nil
}

type eventMeta struct {
	id          *string
	deviceName  string
	profileName string
	sourceName  string
	origin      *int64
	tags        map[string]any
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
	readingMetas map[string]any
}

func newMetaFromMap(m1 map[string]any) *meta {
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
			if v1, ok1 := v.(map[string]any); ok1 {
				result.tags = v1
			}
		default:
			if result.readingMetas == nil {
				result.readingMetas = make(map[string]any)
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
	m1, ok := vi.(map[string]any)
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
	event.ApiVersion = "v4"
	return &event
}

func GetSink() api.Sink {
	return &EdgexMsgBusSink{}
}
