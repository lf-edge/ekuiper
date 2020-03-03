// +build edgex

package extensions

import (
	"context"
	"encoding/json"
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

type EdgexZMQSource struct {
	client     messaging.MessageClient
	vdc        coredata.ValueDescriptorClient
	device     string
	topic      string
	valueDescs map[string]string
}

type EdgexConfig struct {
	Protocol      string `json:"protocol"`
	Server        string `json:"server"`
	Port          int    `json:"port"`
	Topic         string `json:"topic"`
	ServiceServer string `json:"serviceServer"`
}

func (es *EdgexZMQSource) Configure(device string, props map[string]interface{}) error {
	cfg := &EdgexConfig{}
	err := common.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}

	if "" == strings.Trim(device, " ") {
		return fmt.Errorf("Device cannot be empty.")
	} else {
		es.device = device
	}

	if tpc, ok := props["topic"]; ok {
		es.topic = tpc.(string)
	}

	if serviceServer, ok := props["serviceServer"]; ok {
		es.vdc = coredata.NewValueDescriptorClient(local.New(serviceServer.(string) + clients.ApiValueDescriptorRoute))
		es.valueDescs = make(map[string]string)
	} else {
		return fmt.Errorf("The service server cannot be empty.")
	}

	mbconf := types.MessageBusConfig{SubscribeHost: types.HostInfo{Protocol: cfg.Protocol, Host: cfg.Server, Port: cfg.Port}, Type: messaging.ZeroMQ}
	if client, err := messaging.NewMessageClient(mbconf); err != nil {
		return err
	} else {
		es.client = client
		return nil
	}

}

func (es *EdgexZMQSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	log := ctx.GetLogger()
	if err := es.client.Connect(); err != nil {
		errCh <- fmt.Errorf("Failed to connect to message bus: " + err.Error())
	}
	messages := make(chan types.MessageEnvelope)
	topics := []types.TopicChannel{{Topic: es.topic, Messages: messages}}
	err := make(chan error)
	if e := es.client.Subscribe(topics, err); e != nil {
		log.Errorf("Failed to subscribe to topic %s.\n", e)
		errCh <- e
	} else {
		for {
			select {
			case e1 := <-err:
				errCh <- e1
				return
			case env := <-messages:
				if strings.ToLower(env.ContentType) == "application/json" {
					e := models.Event{}
					if err := e.UnmarshalJSON(env.Payload); err != nil {
						log.Warnf("payload %s unmarshal fail: %v", env.Payload, err)
					} else {
						result := make(map[string]interface{})
						meta := make(map[string]interface{})

						log.Debugf("receive message from device %s vs %s", e.Device, es.device)
						if e.Device == es.device {
							for _, r := range e.Readings {
								if r.Name != "" {
									if v, err := es.getValue(r, log); err != nil {
										log.Warnf("fail to get value for %s: %v", r.Name, err)
									} else {
										result[strings.ToLower(r.Name)] = v
									}
								}
							}
							if len(result) > 0 {
								meta["id"] = e.ID
								meta["pushed"] = e.Pushed
								meta["device"] = e.Device
								meta["created"] = e.Created
								meta["modified"] = e.Modified
								meta["origin"] = e.Origin
							} else {
								log.Warnf("got an empty result, ignored")
							}
						}
						//if e := json.Unmarshal(env.Payload, &result); e != nil {
						//	log.Errorf("Invalid data format, cannot convert %s into JSON with error %s", string(env.Payload), e)
						//	return
						//}

						meta["CorrelationID"] = env.CorrelationID
						select {
						case consumer <- api.NewDefaultSourceTuple(result, meta):
							log.Debugf("send data to device node")
						case <-ctx.Done():
							return
						}
					}
				} else {
					log.Errorf("Unsupported data type %s.", env.ContentType)
				}
			}
		}
	}
}

func (es *EdgexZMQSource) getValue(r models.Reading, logger api.Logger) (interface{}, error) {
	t, err := es.getType(r.Name, logger)
	if err != nil {
		return nil, err
	}
	t = strings.ToUpper(t)
	logger.Debugf("name %s with type %s", r.Name, t)
	v := r.Value
	switch t {
	case "B", "BOOL":
		if r, err := strconv.ParseBool(v); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case "I", "INT8", "INT16", "INT32", "INT64", "UINT8", "UINT16", "UINT32", "UINT64":
		if r, err := strconv.Atoi(v); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case "F", "FLOAT16", "FLOAT32", "FLOAT64":
		if r, err := strconv.ParseFloat(v, 64); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	case "S", "STRING":
		return v, nil
	case "J", "JSON":
		var a interface{}
		if err := json.Unmarshal([]byte(v), &a); err != nil {
			return nil, err
		} else {
			return a, nil
		}
	default:
		logger.Warnf("unknown type %s return the string value", t)
		return v, nil
	}
}

func (es *EdgexZMQSource) fetchAllDataDescriptors() error {
	if vdArr, err := es.vdc.ValueDescriptors(context.Background()); err != nil {
		return err
	} else {
		for _, vd := range vdArr {
			es.valueDescs[vd.Id] = vd.Type
		}
	}
	return nil
}

func (es *EdgexZMQSource) getType(id string, logger api.Logger) (string, error) {
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

func (es *EdgexZMQSource) Close(ctx api.StreamContext) error {
	if e := es.client.Disconnect(); e != nil {
		return e
	} else {
		return nil
	}
}
