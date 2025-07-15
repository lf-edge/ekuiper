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
	"encoding/json"
	"fmt"
	"strconv"

	v4 "github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/requests"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/io/edgex/client"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type Source struct {
	cli *client.Client

	config      map[string]any
	topic       string
	messageType messageType
	buflen      int
	conId       string
}

type SourceConf struct {
	Topic       string      `json:"topic"`
	MessageType messageType `json:"messageType"`
	BufferLen   int         `json:"bufferLength"`
}

type SubConf struct {
	Topic string `json:"topic"`
}

type messageType string

const (
	MessageTypeEvent   messageType = "event"
	MessageTypeRequest messageType = "request"
)

func (es *Source) Provision(_ api.StreamContext, props map[string]any) error {
	c := &SourceConf{
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

func (es *Source) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to edgex server")
	var cli *client.Client
	var err error
	id := fmt.Sprintf("%s-%s-%d-edgex-source", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	cw, err := connection.FetchConnection(ctx, id, "edgex", es.config, sc)
	if err != nil {
		return err
	}
	es.conId = cw.ID
	conn, err := cw.Wait(ctx)
	if conn == nil {
		return fmt.Errorf("edgex client not ready: %v", err)
	}
	cli = conn.(*client.Client)
	es.cli = cli
	return err
}

func (es *Source) SubId(props map[string]any) string {
	sc := &SubConf{}
	err := cast.MapToStruct(props, sc)
	if err != nil {
		return ""
	}
	return sc.Topic
}

func (es *Source) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestErr api.ErrorIngest) error {
	log := ctx.GetLogger()

	message := make(chan types.MessageEnvelope, es.buflen)
	errChan := make(chan error)

	if e := es.cli.Subscribe(message, es.topic, errChan); e != nil {
		log.Errorf("Failed to subscribe to edgex messagebus topic %s.", e)
		return e
	} else {
		log.Infof("Successfully subscribed to edgex messagebus topic %s.", es.topic)
		for {
			select {
			case <-ctx.Done():
				log.Infof("Exit subscription to edgex messagebus topic %s.", es.topic)
				return nil
			case e1 := <-errChan:
				log.Errorf("Subscription to edgex messagebus received error %v.", e1)
				ingestErr(ctx, e1)
			case env, ok := <-message:
				rcvTime := timex.GetNow()
				if !ok { // the source is closed
					log.Infof("Exit subscription to edgex messagebus topic %s.", es.topic)
					return nil
				}

				var (
					r   any
					err error
				)
				switch es.messageType {
				case MessageTypeEvent:
					r, err = types.GetMsgPayload[dtos.Event](env)
				case MessageTypeRequest:
					r, err = types.GetMsgPayload[requests.AddEventRequest](env)
				}
				if err != nil {
					log.Errorf("Fail to parse payload: %v", err)
					break
				}

				result := make(map[string]any)
				meta := make(map[string]any)
				var eve dtos.Event
				switch t := r.(type) {
				case dtos.Event:
					eve = t
				case requests.AddEventRequest:
					eve = t.Event
				}

				for _, r := range eve.Readings {
					if r.ResourceName != "" {
						if v, err := es.getValue(r, log); err != nil {
							log.Warnf("fail to get value for %s: %v", r.ResourceName, err)
						} else {
							result[r.ResourceName] = v
						}
						rMeta := map[string]any{}
						rMeta["id"] = r.Id
						// r_meta["created"] = r.Created
						// r_meta["modified"] = r.Modified
						rMeta["origin"] = r.Origin
						// r_meta["pushed"] = r.Pushed
						rMeta["deviceName"] = r.DeviceName
						rMeta["profileName"] = r.ProfileName
						rMeta["valueType"] = r.ValueType
						if r.MediaType != "" {
							rMeta["mediaType"] = r.MediaType
						}
						meta[r.ResourceName] = rMeta
					} else {
						log.Warnf("The name of readings should not be empty!")
					}
				}
				if len(result) > 0 {
					meta["id"] = eve.Id
					// meta["pushed"] = eve.Pushed
					meta["deviceName"] = eve.DeviceName
					meta["profileName"] = eve.ProfileName
					meta["sourceName"] = eve.SourceName
					// meta["created"] = eve.Created
					// meta["modified"] = eve.Modified
					meta["origin"] = eve.Origin
					meta["tags"] = map[string]any(eve.Tags)
					meta["correlationid"] = env.CorrelationID

					ingest(ctx, result, meta, rcvTime)
				} else {
					log.Warnf("No readings are processed for the event, so ignore it.")
				}
			}
		}
	}
}

func (es *Source) getValue(r dtos.BaseReading, logger api.Logger) (any, error) {
	t := r.ValueType
	logger.Debugf("name %s with type %s", r.ResourceName, r.ValueType)
	v := r.Value
	switch t {
	case v4.ValueTypeBool:
		if r, err := strconv.ParseBool(v); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case v4.ValueTypeInt8, v4.ValueTypeInt16, v4.ValueTypeInt32, v4.ValueTypeInt64, v4.ValueTypeUint8, v4.ValueTypeUint16, v4.ValueTypeUint32:
		if r, err := strconv.Atoi(v); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case v4.ValueTypeUint64:
		if u64, err := strconv.ParseUint(v, 10, 64); err != nil {
			return nil, err
		} else {
			return u64, nil
		}
	case v4.ValueTypeFloat32:
		if r, err := strconv.ParseFloat(v, 32); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case v4.ValueTypeFloat64:
		if r, err := strconv.ParseFloat(v, 64); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case v4.ValueTypeString:
		return v, nil
	case v4.ValueTypeBoolArray:
		var val []bool
		if e := json.Unmarshal(cast.StringToBytes(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	case v4.ValueTypeInt8Array, v4.ValueTypeInt16Array, v4.ValueTypeInt32Array, v4.ValueTypeInt64Array, v4.ValueTypeUint8Array, v4.ValueTypeUint16Array, v4.ValueTypeUint32Array:
		var val []int
		if e := json.Unmarshal(cast.StringToBytes(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	case v4.ValueTypeUint64Array:
		var val []uint64
		if e := json.Unmarshal(cast.StringToBytes(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	case v4.ValueTypeFloat32Array:
		return convertFloatArray(v, 32)
	case v4.ValueTypeFloat64Array:
		return convertFloatArray(v, 64)
	case v4.ValueTypeStringArray:
		var val []string
		if e := json.Unmarshal(cast.StringToBytes(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	case v4.ValueTypeBinary:
		return r.BinaryValue, nil
	case v4.ValueTypeObject:
		return r.ObjectValue, nil
	default:
		logger.Warnf("Not supported type %s, and processed as string value", t)
		return v, nil
	}
}

func convertFloatArray(v string, bitSize int) (any, error) {
	var val1 []string
	if e := json.Unmarshal(cast.StringToBytes(v), &val1); e == nil {
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
		if e := json.Unmarshal(cast.StringToBytes(v), &val); e == nil {
			return val, nil
		} else {
			return nil, e
		}
	}
}

func (es *Source) Close(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Infof("EdgeX Source instance %d Done.", ctx.GetInstanceId())
	if es.cli != nil {
		es.cli.DetachSub(ctx, es.config)
		_ = es.cli.Disconnect()
	}
	return connection.DetachConnection(ctx, es.conId)
}

func GetSource() api.Source {
	return &Source{}
}
