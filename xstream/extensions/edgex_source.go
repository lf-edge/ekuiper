// +build edgex

package extensions

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/clients"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/coredata"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/urlclient/local"
	"github.com/edgexfoundry/go-mod-core-contracts/models"
	"github.com/edgexfoundry/go-mod-messaging/messaging"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"strconv"
	"strings"
)

type EdgexSource struct {
	client     messaging.MessageClient
	subscribed bool
	vdc        coredata.ValueDescriptorClient
	topic      string
	valueDescs map[string]string
}

func (es *EdgexSource) Configure(device string, props map[string]interface{}) error {
	var protocol = "tcp";
	if p, ok := props["protocol"]; ok {
		protocol = p.(string)
	}
	var server = "localhost";
	if s, ok := props["server"]; ok {
		server = s.(string)
	}
	var port = 5563
	if p, ok := props["port"]; ok {
		port = p.(int)
	}

	if tpc, ok := props["topic"]; ok {
		es.topic = tpc.(string)
	}

	var mbusType = messaging.ZeroMQ
	if t, ok := props["type"]; ok {
		mbusType = t.(string)
		if mbusType != messaging.ZeroMQ && mbusType != messaging.MQTT {
			return fmt.Errorf("Specified wrong message type value %s, will use zeromq messagebus.\n", mbusType)
		}
	}

	if serviceServer, ok := props["serviceServer"]; ok {
		svr := serviceServer.(string) + clients.ApiValueDescriptorRoute
		common.Log.Infof("Connect to value descriptor service at: %s \n", svr)
		es.vdc = coredata.NewValueDescriptorClient(local.New(svr))
		es.valueDescs = make(map[string]string)
	} else {
		return fmt.Errorf("The service server cannot be empty.")
	}

	mbconf := types.MessageBusConfig{SubscribeHost: types.HostInfo{Protocol: protocol, Host: server, Port: port}, Type: mbusType}

	var optional = make(map[string]string)
	if ops, ok := props["optional"]; ok {
		if ops1, ok1 := ops.(map[string]interface{}); ok1 {
			for k, v := range ops1 {
				if cv, ok := CastToString(v); ok {
					optional[k] = cv
				} else {
					common.Log.Infof("Cannot convert configuration %s: %s to string type.\n", k, v)
				}
			}
		}
		mbconf.Optional = optional
	}
	common.Log.Infof("Use configuration for edgex messagebus %v\n", mbconf)

	if client, err := messaging.NewMessageClient(mbconf); err != nil {
		return err
	} else {
		es.client = client
		return nil
	}

}

func castToString(v interface{}) (result string, ok bool) {
	switch v := v.(type) {
	case int:
		return strconv.Itoa(v), true
	case string:
		return v, true
	case bool:
		return strconv.FormatBool(v), true
	case float64, float32:
		return fmt.Sprintf("%.2f", v), true
	default:
		return "", false
	}
}

func (es *EdgexSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	log := ctx.GetLogger()
	if err := es.client.Connect(); err != nil {
		info := fmt.Errorf("Failed to connect to edgex message bus: " + err.Error())
		log.Errorf(info.Error())
		errCh <- info
		return
	}
	log.Infof("The connection to edgex messagebus is established successfully.")
	messages := make(chan types.MessageEnvelope)
	topics := []types.TopicChannel{{Topic: es.topic, Messages: messages}}
	err := make(chan error)
	if e := es.client.Subscribe(topics, err); e != nil {
		log.Errorf("Failed to subscribe to edgex messagebus topic %s.\n", e)
		errCh <- e
	} else {
		es.subscribed = true
		log.Infof("Successfully subscribed to edgex messagebus topic %s.", es.topic)
		for {
			select {
			case e1 := <-err:
				errCh <- e1
				return
			case env := <-messages:
				if strings.ToLower(env.ContentType) == "application/json" {
					e := models.Event{}
					if err := e.UnmarshalJSON(env.Payload); err != nil {
						len := len(env.Payload)
						if len > 200 {
							len = 200
						}
						log.Warnf("payload %s unmarshal fail: %v", env.Payload[0:(len - 1)], err)
					} else {
						result := make(map[string]interface{})
						meta := make(map[string]interface{})

						log.Debugf("receive message %s from device %s", env.Payload, e.Device)
						for _, r := range e.Readings {
							if r.Name != "" {
								if v, err := es.getValue(r, log); err != nil {
									log.Warnf("fail to get value for %s: %v", r.Name, err)
								} else {
									result[r.Name] = v
								}
								r_meta := map[string]interface{}{}
								r_meta["id"] = r.Id
								r_meta["created"] = r.Created
								r_meta["modified"] = r.Modified
								r_meta["origin"] = r.Origin
								r_meta["pushed"] = r.Pushed
								r_meta["device"] = r.Device
								meta[r.Name] = r_meta
							} else {
								log.Warnf("The name of readings should not be empty!")
							}
						}
						if len(result) > 0 {
							meta["id"] = e.ID
							meta["pushed"] = e.Pushed
							meta["device"] = e.Device
							meta["created"] = e.Created
							meta["modified"] = e.Modified
							meta["origin"] = e.Origin
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

func (es *EdgexSource) getValue(r models.Reading, logger api.Logger) (interface{}, error) {
	t, err := es.getType(r.Name, logger)
	var ot = t
	if err != nil {
		return nil, err
	}
	t = strings.ToUpper(t)
	logger.Debugf("name %s with type %s", r.Name, t)
	v := r.Value
	switch t {
	case "BOOL":
		if r, err := strconv.ParseBool(v); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case "INT8", "INT16", "INT32", "INT64", "UINT8", "UINT16", "UINT32":
		if r, err := strconv.Atoi(v); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case "UINT64":
		if u64, err := strconv.ParseUint(v, 10, 64); err != nil {
			return nil, err
		} else {
			return u64, nil
		}
	case "FLOAT32", "FLOAT64":
		if r.ValueType == "" {
			r.ValueType = ot
		}
		return es.getFloatValue(r, logger)
	case "STRING":
		return v, nil
	case "BINARY":
		return nil, fmt.Errorf("Unsupport for binary type, the value will be ignored.")
	default:
		logger.Warnf("Not supported type %s, and processed as string value", t)
		return v, nil
	}
}

func (es *EdgexSource) getFloatValue(r models.Reading, logger api.Logger) (interface{}, error) {
	if len(r.FloatEncoding) == 0 {
		if strings.Contains(r.Value, "=") {
			r.FloatEncoding = models.Base64Encoding
		} else {
			r.FloatEncoding = models.ENotation
		}
	}
	switch strings.ToLower(r.ValueType) {
	case strings.ToLower(models.ValueTypeFloat32):
		var value float64
		switch r.FloatEncoding {
		case models.Base64Encoding:
			data, err := base64.StdEncoding.DecodeString(r.Value)
			if err != nil {
				return false, fmt.Errorf("unable to Base 64 decode float32 value ('%s'): %s", r.Value, err.Error())
			}
			var value1 float32
			err = binary.Read(bytes.NewReader(data), binary.BigEndian, &value1)
			if err != nil {
				return false, fmt.Errorf("unable to decode float32 value bytes: %s", err.Error())
			}
			value = float64(value1)
		case models.ENotation:
			var err error
			var temp float64
			temp, err = strconv.ParseFloat(r.Value, 64)
			if err != nil {
				return false, fmt.Errorf("unable to parse Float64 eNotation value: %s", err.Error())
			}

			value = float64(temp)

		default:
			return false, fmt.Errorf("unkown FloatEncoding for float32 value: %s", r.FloatEncoding)

		}
		return value, nil

	case strings.ToLower(models.ValueTypeFloat64):
		var value float64
		switch r.FloatEncoding {
		case models.Base64Encoding:
			data, err := base64.StdEncoding.DecodeString(r.Value)
			if err != nil {
				return false, fmt.Errorf("unable to Base 64 decode float64 value ('%s'): %s", r.Value, err.Error())
			}

			err = binary.Read(bytes.NewReader(data), binary.BigEndian, &value)
			if err != nil {
				return false, fmt.Errorf("unable to decode float64 value bytes: %s", err.Error())
			}
			return value, nil
		case models.ENotation:
			var err error
			value, err = strconv.ParseFloat(r.Value, 64)
			if err != nil {
				return false, fmt.Errorf("unable to parse Float64 eNotation value: %s", err.Error())
			}
			return value, nil
		default:
			return false, fmt.Errorf("unkown FloatEncoding for float64 value: %s", r.FloatEncoding)
		}
	default:
		return nil, fmt.Errorf("unkown value type: %s, reading:%v", r.ValueType, r)
	}
}


func (es *EdgexSource) fetchAllDataDescriptors() error {
	if vdArr, err := es.vdc.ValueDescriptors(context.Background()); err != nil {
		return err
	} else {
		for _, vd := range vdArr {
			es.valueDescs[vd.Name] = vd.Type
		}
		if len(vdArr) == 0 {
			common.Log.Infof("Cannot find any value descriptors from value descriptor services.")
		} else {
			common.Log.Infof("Get %d of value descriptors from service.", len(vdArr))
			for i, v := range vdArr {
				common.Log.Debugf("%d: %s - %s ", i, v.Name, v.Type)
			}
		}
	}
	return nil
}

func (es *EdgexSource) getType(id string, logger api.Logger) (string, error) {
	if t, ok := es.valueDescs[id]; ok {
		return t, nil
	} else {
		if e := es.fetchAllDataDescriptors(); e != nil {
			return "", e
		}
		if t, ok := es.valueDescs[id]; ok {
			return t, nil
		} else {
			return "", fmt.Errorf("cannot find type info for %s in value descriptor.", id)
		}
	}
}

func (es *EdgexSource) Close(ctx api.StreamContext) error {
	if es.subscribed {
		if e := es.client.Disconnect(); e != nil {
			return e
		}
	}
	return nil
}