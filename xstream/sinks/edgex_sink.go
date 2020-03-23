// +build edgex

package sinks

import (
	"fmt"
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

	optional *OptionalConf
	client   messaging.MessageClient
}

type OptionalConf struct {
	clientid string
	username string
	password string
}

func (ems *EdgexMsgBusSink) Configure(ps map[string]interface{}) error {
	ems.host = "*"
	ems.protocol = "tcp"
	ems.port = 5570
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
			common.Log.Infof("Not valid port value, will use default value '5570'.")
		}

	} else {
		common.Log.Infof("Not find port conf, will use default value '5570'.")
	}

	if topic, ok := ps["topic"]; ok {
		ems.topic = topic.(string)
	} else {
		return fmt.Errorf("Topic must be specified.")
	}

	if contentType, ok := ps["contentType"]; ok {
		ems.contentType = contentType.(string)
	} else {
		common.Log.Infof("Not find contentType conf, will use default value 'application/json'.")
	}

	if optIntf, ok := ps["optional"]; ok {
		if opt, ok1 := optIntf.(map[string]interface{}); ok1 {
			optional := &OptionalConf{}
			ems.optional = optional
			if cid, ok2 := opt["clientid"]; ok2 {
				optional.clientid = cid.(string)
			}
			if uname, ok2 := opt["username"]; ok2 {
				optional.username = uname.(string)
			}
			if password, ok2 := opt["password"]; ok2 {
				optional.password = password.(string)
			}
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

func (ems *EdgexMsgBusSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if payload, ok := item.([]byte); ok {
		logger.Debugf("EdgeX message bus sink: %s\n", payload)
		env := types.NewMessageEnvelope(payload, ctx)
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
