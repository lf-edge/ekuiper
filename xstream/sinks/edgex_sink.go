
package sinks

import (
	"encoding/json"
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/coredata"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/urlclient/local"
	"github.com/edgexfoundry/go-mod-core-contracts/models"
	"github.com/edgexfoundry/go-mod-messaging/messaging"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
)

type EdgexMsgBusSink struct {
	protocol string
	host     string
	port     int
	ptype    string

	topic       string
	contentType string

	deviceName string
	metadata   string

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
		common.Log.Infof("Not find host conf, will use default value '*'.")
	}

	if pro, ok := ps["protocol"]; ok {
		ems.protocol = pro.(string)
	} else {
		common.Log.Infof("Not find protocol conf, will use default value 'tcp'.")
	}

	if port, ok := ps["port"]; ok {
		if pv, ok := port.(float64); ok {
			ems.port = int(pv)
		} else if pv, ok := port.(float32); ok {
			ems.port = int(pv)
		} else {
			common.Log.Infof("Not valid port value, will use default value '5563'.")
		}

	} else {
		common.Log.Infof("Not find port conf, will use default value '5563'.")
	}

	if topic, ok := ps["topic"]; ok {
		ems.topic = topic.(string)
	} else {
		common.Log.Infof("Not find topic conf, will use default value 'events'.")
	}

	if contentType, ok := ps["contentType"]; ok {
		ems.contentType = contentType.(string)
	} else {
		common.Log.Infof("Not find contentType conf, will use default value 'application/json'.")
	}

	if ptype, ok := ps["type"]; ok {
		ems.ptype = ptype.(string)
		if ems.ptype != messaging.ZeroMQ && ems.ptype != messaging.MQTT {
			common.Log.Infof("Specified wrong message type value %s, will use zeromq messagebus.\n", ems.ptype)
			ems.ptype = messaging.ZeroMQ
		}
	}

	if dname, ok := ps["deviceName"]; ok {
		ems.deviceName = dname.(string)
	}

	if metadata, ok := ps["metadata"]; ok {
		ems.metadata = metadata.(string)
	}

	if optIntf, ok := ps["optional"]; ok {
		if opt, ok1 := optIntf.(map[string]interface{}); ok1 {
			optional := make(map[string]string)
			for k, v := range opt {
				//if !xstream.IsAllowedEdgeOptionalKeys(k) {
				//	return fmt.Errorf("The optional key %s is not allowed. ", k)
				//}
				if sv, ok2 := v.(string); ok2 {
					optional[k] = sv
				} else {
					info := fmt.Sprintf("Only string value is allowed for optional value, the value for key %s is not a string.", k)
					common.Log.Infof(info)
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
		Type: ems.ptype,
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

func (ems *EdgexMsgBusSink) produceEvents(result []byte) (*models.Event, error) {
	var m []map[string]interface{}
	if err := json.Unmarshal(result, &m); err == nil {
		m1, f := ems.getMeta(m)
		var event = &models.Event{}
		if f {
			event.Device = m1.getStrVal("device")
			event.Created = m1.getIntVal("created")
			event.Modified = m1.getIntVal("modified")
			event.Origin = m1.getIntVal("origin")
			event.ID = m1.getStrVal("id")
			event.Pushed = m1.getIntVal("pushed")
		}
		//Override the devicename if user specified the value
		if ems.deviceName != "" {
			event.Device = ems.deviceName
		}

		for _, v := range m {
			for k1, v1 := range v {
				if k1 == ems.metadata {
					continue
				} else {
					value := fmt.Sprintf("%v", v1)
					r := models.Reading{Name: k1, Value: value}
					if m, ok := m1[k1]; ok {
						if mm, ok1 := m.(map[string]interface{}); ok1 {
							mm1 := meta(mm)
							r.Created = mm1.getIntVal("created")
							r.Device = mm1.getStrVal("device")
							r.Id = mm1.getStrVal("id")
							r.Modified = mm1.getIntVal("modified")
							r.Origin = mm1.getIntVal("origin")
							r.Pushed = mm1.getIntVal("pushed")
						}
					}
					event.Readings = append(event.Readings, r)
				}
			}
		}
		return event, nil
	} else {
		return nil, err
	}
}

type meta map[string]interface{}

func (ems *EdgexMsgBusSink) getMeta(result []map[string]interface{}) (meta, bool) {
	if ems.metadata == "" {
		return nil, false
	}
	//Try to get the meta field
	for _, v := range result {
		if m, ok := v[ems.metadata]; ok {
			if m1, ok1 := m.(map[string]interface{}); ok1 {
				return meta(m1), true
			} else {
				common.Log.Infof("Specified a meta field, but the field does not contains any EdgeX metadata.")
			}
		}
	}
	return nil, false
}

func (m meta) getIntVal(k string) (int64) {
	if v, ok := m[k]; ok {
		if v1, ok1 := v.(float64); ok1 {
			return int64(v1)
		}
	}
	return 0
}

func (m meta) getStrVal(k string) (string) {
	if v, ok := m[k]; ok {
		if v1, ok1 := v.(string); ok1 {
			return v1
		}
	}
	return ""
}

func (ems *EdgexMsgBusSink) getMetaValueAsMap(m meta, k string) (map[string]interface{}) {
	if v, ok := m[k]; ok {
		if v1, ok1 := v.(map[string]interface{}); ok1 {
			return v1
		}
	}
	return nil
}

func (ems *EdgexMsgBusSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	client := coredata.NewEventClient(local.New(""))
	if payload, ok := item.([]byte); ok {
		logger.Debugf("EdgeX message bus sink: %s\n", payload)
		evt, err := ems.produceEvents(payload)
		if err != nil {
			return fmt.Errorf("Failed to convert to EdgeX event: %s.", err.Error())
		}
		data, err := client.MarshalEvent(*evt)
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
