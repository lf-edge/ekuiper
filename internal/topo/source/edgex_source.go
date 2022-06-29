// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package source

import (
	"fmt"
	v2 "github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos/requests"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"strconv"
	"strings"
)

type EdgexSource struct {
	cli api.MessageClient

	config      map[string]interface{}
	topic       string
	messageType messageType
	buflen      int
}

type EdgexConf struct {
	Topic       string      `json:"topic"`
	MessageType messageType `json:"messageType"`
	BufferLen   int         `json:"bufferLength"`
}

type messageType string

const (
	MessageTypeEvent   messageType = "event"
	MessageTypeRequest messageType = "request"
)

func (es *EdgexSource) Configure(_ string, props map[string]interface{}) error {
	c := &EdgexConf{
		MessageType: MessageTypeEvent,
	}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if c.BufferLen <= 0 {
		c.BufferLen = 1024
	}
	es.buflen = c.BufferLen
	es.messageType = c.MessageType
	es.topic = c.Topic
	es.config = props

	return nil
}

func (es *EdgexSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	log := ctx.GetLogger()

	cli, err := ctx.GetClient("edgex", es.config)
	if err != nil {
		errCh <- err
		log.Errorf("found error when get edgex client, error %s", err.Error())
		return
	}
	es.cli = cli.(api.MessageClient)

	messages := make(chan interface{}, es.buflen)
	topics := []api.TopicChannel{{Topic: es.topic, Messages: messages}}
	subErrs := make(chan error, len(topics))
	if e := es.cli.Subscribe(ctx, topics, subErrs, nil); e != nil {
		log.Errorf("Failed to subscribe to edgex messagebus topic %s.\n", e)
		errCh <- e
	} else {
		log.Infof("Successfully subscribed to edgex messagebus topic %s.", es.topic)
		for {
			select {
			case <-ctx.Done():
				log.Infof("Exit subscription to edgex messagebus topic %s.", es.topic)
				return
			case e1 := <-subErrs:
				errCh <- e1
				return
			case msg, ok := <-messages:
				if !ok { // the source is closed
					log.Infof("Exit subscription to edgex messagebus topic %s.", es.topic)
					return
				}

				env, ok := msg.(*types.MessageEnvelope)
				if !ok {
					log.Errorf("can not convert interface data to mqtt message.")
					return
				}

				if strings.EqualFold(env.ContentType, "application/json") {
					var r interface{}
					switch es.messageType {
					case MessageTypeEvent:
						r = &dtos.Event{}
					case MessageTypeRequest:
						r = &requests.AddEventRequest{}
					}
					if err := message.Unmarshal(env.Payload, r); err != nil {
						l := len(env.Payload)
						if l > 200 {
							l = 200
						}
						log.Warnf("payload %s unmarshal fail: %v", env.Payload[0:(l-1)], err)
					} else {
						result := make(map[string]interface{})
						meta := make(map[string]interface{})
						var e *dtos.Event
						switch t := r.(type) {
						case *dtos.Event:
							e = t
						case *requests.AddEventRequest:
							e = &t.Event
						}

						log.Debugf("receive message %s from device %s", env.Payload, e.DeviceName)
						for _, r := range e.Readings {
							if r.ResourceName != "" {
								if v, err := es.getValue(r, log); err != nil {
									log.Warnf("fail to get value for %s: %v", r.ResourceName, err)
								} else {
									result[r.ResourceName] = v
								}
								r_meta := map[string]interface{}{}
								r_meta["id"] = r.Id
								//r_meta["created"] = r.Created
								//r_meta["modified"] = r.Modified
								r_meta["origin"] = r.Origin
								//r_meta["pushed"] = r.Pushed
								r_meta["deviceName"] = r.DeviceName
								r_meta["profileName"] = r.ProfileName
								r_meta["valueType"] = r.ValueType
								if r.MediaType != "" {
									r_meta["mediaType"] = r.MediaType
								}
								meta[r.ResourceName] = r_meta
							} else {
								log.Warnf("The name of readings should not be empty!")
							}
						}
						if len(result) > 0 {
							meta["id"] = e.Id
							//meta["pushed"] = e.Pushed
							meta["deviceName"] = e.DeviceName
							meta["profileName"] = e.ProfileName
							meta["sourceName"] = e.SourceName
							//meta["created"] = e.Created
							//meta["modified"] = e.Modified
							meta["origin"] = e.Origin
							meta["tags"] = e.Tags
							meta["correlationid"] = env.CorrelationID

							select {
							case consumer <- api.NewDefaultSourceTuple(result, meta):
								log.Debugf("send data to device node")
							case <-ctx.Done():
								return
							}
						} else {
							log.Warnf("No readings are processed for the event, so ignore it.")
						}
					}
				} else {
					log.Errorf("Unsupported data type %s.", env.ContentType)
				}
			}
		}
	}
}

func (es *EdgexSource) getValue(r dtos.BaseReading, logger api.Logger) (interface{}, error) {
	t := r.ValueType
	logger.Debugf("name %s with type %s", r.ResourceName, r.ValueType)
	v := r.Value
	switch t {
	case v2.ValueTypeBool:
		if r, err := strconv.ParseBool(v); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case v2.ValueTypeInt8, v2.ValueTypeInt16, v2.ValueTypeInt32, v2.ValueTypeInt64, v2.ValueTypeUint8, v2.ValueTypeUint16, v2.ValueTypeUint32:
		if r, err := strconv.Atoi(v); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case v2.ValueTypeUint64:
		if u64, err := strconv.ParseUint(v, 10, 64); err != nil {
			return nil, err
		} else {
			return u64, nil
		}
	case v2.ValueTypeFloat32:
		if r, err := strconv.ParseFloat(v, 32); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case v2.ValueTypeFloat64:
		if r, err := strconv.ParseFloat(v, 64); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case v2.ValueTypeString:
		return v, nil
	case v2.ValueTypeBoolArray:
		var val []bool
		if e := message.Unmarshal([]byte(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	case v2.ValueTypeInt8Array, v2.ValueTypeInt16Array, v2.ValueTypeInt32Array, v2.ValueTypeInt64Array, v2.ValueTypeUint8Array, v2.ValueTypeUint16Array, v2.ValueTypeUint32Array:
		var val []int
		if e := message.Unmarshal([]byte(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	case v2.ValueTypeUint64Array:
		var val []uint64
		if e := message.Unmarshal([]byte(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	case v2.ValueTypeFloat32Array:
		return convertFloatArray(v, 32)
	case v2.ValueTypeFloat64Array:
		return convertFloatArray(v, 64)
	case v2.ValueTypeStringArray:
		var val []string
		if e := message.Unmarshal([]byte(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	case v2.ValueTypeBinary:
		return r.BinaryValue, nil
	case v2.ValueTypeObject:
		return r.ObjectValue, nil
	default:
		logger.Warnf("Not supported type %s, and processed as string value", t)
		return v, nil
	}
}

func convertFloatArray(v string, bitSize int) (interface{}, error) {
	var val1 []string
	if e := message.Unmarshal([]byte(v), &val1); e == nil {
		var ret []float64
		for _, v := range val1 {
			if fv, err := strconv.ParseFloat(v, bitSize); err != nil {
				return nil, err
			} else {
				ret = append(ret, fv)
			}
		}
		return ret, nil
	} else {
		var val []float64
		if e := message.Unmarshal([]byte(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	}
}

func (es *EdgexSource) Close(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Infof("EdgeX Source instance %d Done.", ctx.GetInstanceId())
	if es.cli != nil {
		es.cli.Release(ctx)
	}
	return nil
}
