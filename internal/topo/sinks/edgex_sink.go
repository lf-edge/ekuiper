// +build edgex

package sinks

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	v2 "github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/emqx/kuiper/internal/conf"
	"github.com/emqx/kuiper/pkg/api"
	"github.com/emqx/kuiper/pkg/cast"
	"reflect"
)

type EdgexMsgBusSink struct {
	protocol string
	host     string
	port     int
	ptype    string

	topic       string
	contentType string

	deviceName  string
	profileName string
	sourceName  string
	metadata    string

	optional map[string]string
	client   messaging.MessageClient
}

func (ems *EdgexMsgBusSink) Configure(ps map[string]interface{}) error {
	ems.host = "*"
	ems.protocol = "tcp"
	ems.port = 5573
	ems.topic = "events"
	ems.contentType = "application/json"
	ems.ptype = messaging.ZeroMQ

	if host, ok := ps["host"]; ok {
		ems.host = host.(string)
	} else {
		conf.Log.Infof("Not find host conf, will use default value '*'.")
	}

	if pro, ok := ps["protocol"]; ok {
		ems.protocol = pro.(string)
	} else {
		conf.Log.Infof("Not find protocol conf, will use default value 'tcp'.")
	}

	if port, ok := ps["port"]; ok {
		if pv, ok := port.(float64); ok {
			ems.port = int(pv)
		} else if pv, ok := port.(float32); ok {
			ems.port = int(pv)
		} else {
			conf.Log.Infof("Not valid port value, will use default value '5563'.")
		}

	} else {
		conf.Log.Infof("Not find port conf, will use default value '5563'.")
	}

	if topic, ok := ps["topic"]; ok {
		ems.topic = topic.(string)
	} else {
		conf.Log.Infof("Not find topic conf, will use default value 'events'.")
	}

	if contentType, ok := ps["contentType"]; ok {
		ems.contentType = contentType.(string)
	} else {
		conf.Log.Infof("Not find contentType conf, will use default value 'application/json'.")
	}

	if ptype, ok := ps["type"]; ok {
		ems.ptype = ptype.(string)
		if ems.ptype != messaging.ZeroMQ && ems.ptype != messaging.MQTT && ems.ptype != messaging.Redis {
			conf.Log.Infof("Specified wrong message type value %s, will use zeromq messagebus.\n", ems.ptype)
			ems.ptype = messaging.ZeroMQ
		}
	}

	if dname, ok := ps["deviceName"]; ok {
		ems.deviceName = dname.(string)
	}

	if pname, ok := ps["profileName"]; ok {
		ems.profileName = pname.(string)
	}

	if metadata, ok := ps["metadata"]; ok {
		ems.metadata = metadata.(string)
	}

	if optIntf, ok := ps["optional"]; ok {
		if opt, ok1 := optIntf.(map[string]interface{}); ok1 {
			optional := make(map[string]string)
			for k, v := range opt {
				if sv, ok2 := v.(string); ok2 {
					optional[k] = sv
				} else {
					info := fmt.Sprintf("Only string value is allowed for optional value, the value for key %s is not a string.", k)
					conf.Log.Infof(info)
					return fmt.Errorf(info)
				}
			}
			ems.optional = optional
		}
	}
	return nil
}

func (ems *EdgexMsgBusSink) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	conf := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     ems.host,
			Port:     ems.port,
			Protocol: ems.protocol,
		},
		Type:     ems.ptype,
		Optional: ems.optional,
	}
	log.Infof("Using configuration for EdgeX message bus sink: %+v", conf)
	if msgClient, err := messaging.NewMessageClient(conf); err != nil {
		return err
	} else {
		if ec := msgClient.Connect(); ec != nil {
			return ec
		} else {
			ems.client = msgClient
		}
	}
	return nil
}

func (ems *EdgexMsgBusSink) produceEvents(ctx api.StreamContext, result []byte) (*dtos.Event, error) {
	var m []map[string]interface{}
	if err := json.Unmarshal(result, &m); err == nil {
		m1 := ems.getMeta(m)
		event := m1.createEvent()
		//Override the devicename if user specified the value
		if ems.deviceName != "" {
			event.DeviceName = ems.deviceName
		}
		if ems.profileName != "" {
			event.ProfileName = ems.profileName
		}
		if event.SourceName == "" {
			event.SourceName = ems.topic
		}
		for _, v := range m {
			for k1, v1 := range v {
				if k1 == ems.metadata {
					continue
				} else {
					var (
						vt string
						vv interface{}
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
	} else {
		return nil, err
	}
}

func getValueType(v interface{}) (string, interface{}, error) {
	k := reflect.TypeOf(v).Kind()
	switch k {
	case reflect.Bool:
		return v2.ValueTypeBool, v, nil
	case reflect.String:
		return v2.ValueTypeString, v, nil
	case reflect.Int64:
		return v2.ValueTypeInt64, v, nil
	case reflect.Int:
		return v2.ValueTypeInt64, v, nil
	case reflect.Float64:
		return v2.ValueTypeFloat64, v, nil
	case reflect.Slice:
		switch arrayValue := v.(type) {
		case []interface{}:
			if len(arrayValue) > 0 {
				ka := reflect.TypeOf(arrayValue[0]).Kind()
				switch ka {
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
		default:
			return "", nil, fmt.Errorf("unable to cast value to []interface{} for %v", v)
		}
	}
	return "", nil, fmt.Errorf("unsupported value %v(%s)", v, k)
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
	default:
		return nil, fmt.Errorf("unsupported type %v", vt)
	}
}

func (ems *EdgexMsgBusSink) getMeta(result []map[string]interface{}) *meta {
	if ems.metadata == "" {
		return newMetaFromMap(nil)
	}
	//Try to get the meta field
	for _, v := range result {
		if m, ok := v[ems.metadata]; ok {
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
	if payload, ok := item.([]byte); ok {
		logger.Debugf("EdgeX message bus sink: %s\n", payload)
		evt, err := ems.produceEvents(ctx, payload)
		if err != nil {
			return fmt.Errorf("Failed to convert to EdgeX event: %s.", err.Error())
		}
		data, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("unexpected error MarshalEvent %v", err)
		}
		env := types.NewMessageEnvelope([]byte(data), ctx)
		env.ContentType = ems.contentType

		if e := ems.client.Publish(env, ems.topic); e != nil {
			logger.Errorf("Found error %s when publish to EdgeX message bus.\n", e)
			return e
		}
	} else {
		return fmt.Errorf("Unkown type %t, the message cannot be published.\n", item)
	}
	return nil
}

func (ems *EdgexMsgBusSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing edgex sink")
	if ems.client != nil {
		if e := ems.client.Disconnect(); e != nil {
			return e
		}
	}
	return nil
}

type eventMeta struct {
	id          *string
	deviceName  string
	profileName string
	sourceName  string
	origin      *int64
	tags        map[string]string
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
		eventMeta: eventMeta{
			profileName: "kuiperProfile",
		},
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
				r := make(map[string]string)
				for k, vi := range v1 {
					s, ok := vi.(string)
					if ok {
						r[k] = s
					}
				}
				result.tags = r
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
