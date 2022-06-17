// Copyright 2022 EMQ Technologies Co., Ltd.
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

package mqtt

import (
	"fmt"
	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"strings"
	"sync"
)

type mqttSubscriptionInfo struct {
	topic        string
	qos          byte
	topicHandler pahoMqtt.MessageHandler

	topicConsumers []*clients.ConsumerInfo
}

type mqttClientWrapper struct {
	cli *MQTTClient

	subLock sync.RWMutex
	//topic: subscriber
	//multiple go routine can sub same topic
	topicSubscriptions map[string]*mqttSubscriptionInfo

	//consumerId: SubscribedTopics
	subscribers map[string]clients.SubscribedTopics

	conSelector string

	connected bool

	refLock sync.RWMutex
	refCnt  uint64
}

func NewMqttClientWrapper(props map[string]interface{}) (clients.ClientWrapper, error) {
	if props == nil {
		conf.Log.Warnf("props is nill for mqtt client wrapper")
	}
	client := &MQTTClient{}
	err := client.CfgValidate(props)
	if err != nil {
		return nil, err
	}
	cliWpr := &mqttClientWrapper{
		cli:                client,
		subLock:            sync.RWMutex{},
		topicSubscriptions: make(map[string]*mqttSubscriptionInfo),
		subscribers:        make(map[string]clients.SubscribedTopics),
		refCnt:             1,
	}

	err = client.Connect(cliWpr.onConnectHandler, cliWpr.onConnectLost)
	if err != nil {
		return nil, err
	}

	return cliWpr, nil
}

func (mc *mqttClientWrapper) onConnectHandler(_ pahoMqtt.Client) {
	// activeSubscriptions will be empty on the first connection.
	// On a re-connect is when the subscriptions must be re-created.
	conf.Log.Infof("The connection to mqtt broker %s client id %s established", mc.cli.srv, mc.cli.clientid)
	mc.subLock.Lock()
	defer mc.subLock.Unlock()
	mc.connected = true
	for topic, subscription := range mc.topicSubscriptions {
		token := mc.cli.conn.Subscribe(topic, subscription.qos, subscription.topicHandler)
		if token.Error() != nil {
			for _, con := range subscription.topicConsumers {
				con.SubErrors <- token.Error()
			}
		}
	}
}

func (mc *mqttClientWrapper) onConnectLost(_ pahoMqtt.Client, err error) {
	mc.subLock.Lock()
	defer mc.subLock.Unlock()
	mc.connected = false
	conf.Log.Warnf("The connection to mqtt broker %s client id %s disconnected with error: %s ", mc.cli.srv, mc.cli.clientid, err.Error())
}

func (mc *mqttClientWrapper) newMessageHandler(sub *mqttSubscriptionInfo) pahoMqtt.MessageHandler {
	return func(client pahoMqtt.Client, message pahoMqtt.Message) {
		if sub != nil {
			// broadcast to all consumers
			for _, consumer := range sub.topicConsumers {
				select {
				case consumer.ConsumerChan <- message:
					break
				default:
					conf.Log.Warnf("consumer chan full for request id %s", consumer.ConsumerId)
				}
			}
		}
	}
}

func (mc *mqttClientWrapper) Publish(_ api.StreamContext, topic string, message []byte, params map[string]interface{}) error {
	err := mc.checkConn()
	if err != nil {
		return err
	}
	var Qos byte = 0
	if pq, ok := params["qos"]; ok {
		if v, ok := pq.(byte); ok {
			Qos = v
		}
	}
	retained := false
	if pk, ok := params["retained"]; ok {
		if v, ok := pk.(bool); ok {
			retained = v
		}
	}

	err = mc.cli.Publish(topic, Qos, retained, message)
	if err != nil {
		return err
	}

	return nil
}

func (mc *mqttClientWrapper) checkConn() error {
	mc.subLock.RLock()
	defer mc.subLock.RUnlock()
	if !mc.connected {
		return fmt.Errorf("%s: %s", errorx.IOErr, "mqtt client is not connected")
	}
	return nil
}

func (mc *mqttClientWrapper) Subscribe(c api.StreamContext, subChan []api.TopicChannel, messageErrors chan error, params map[string]interface{}) error {
	log := c.GetLogger()

	mc.subLock.Lock()
	defer mc.subLock.Unlock()

	subId := fmt.Sprintf("%s_%s_%d", c.GetRuleId(), c.GetOpId(), c.GetInstanceId())
	if _, ok := mc.subscribers[subId]; ok {
		return fmt.Errorf("already have subscription %s", subId)
	}

	var Qos byte = 0
	if pq, ok := params["qos"]; ok {
		if v, ok := pq.(byte); ok {
			Qos = v
		}
	}

	subTopics := clients.SubscribedTopics{
		Topics: make([]string, 0),
	}

	for _, tpChan := range subChan {
		tpc := tpChan.Topic
		subTopics.Topics = append(subTopics.Topics, tpc)
		sub, found := mc.topicSubscriptions[tpc]
		if found {
			sub.topicConsumers = append(sub.topicConsumers, &clients.ConsumerInfo{
				ConsumerId:   subId,
				ConsumerChan: tpChan.Messages,
				SubErrors:    messageErrors,
			})
			log.Infof("subscription for topic %s already exists, reqId is %s, total subs %d", tpc, subId, len(sub.topicConsumers))
		} else {
			sub := &mqttSubscriptionInfo{
				topic: tpc,
				qos:   Qos,
				topicConsumers: []*clients.ConsumerInfo{
					{
						ConsumerId:   subId,
						ConsumerChan: tpChan.Messages,
						SubErrors:    messageErrors,
					},
				},
			}
			sub.topicHandler = mc.newMessageHandler(sub)
			log.Infof("new subscription for topic %s, reqId is %s", tpc, subId)
			token := mc.cli.conn.Subscribe(tpc, Qos, sub.topicHandler)
			if token.Error() != nil {
				messageErrors <- token.Error()
				return token.Error()
			}
			mc.topicSubscriptions[tpc] = sub
		}
	}
	mc.subscribers[subId] = subTopics

	return nil
}

func (mc *mqttClientWrapper) unsubscribe(c api.StreamContext) {
	log := c.GetLogger()
	mc.subLock.Lock()
	defer mc.subLock.Unlock()

	subId := fmt.Sprintf("%s_%s_%d", c.GetRuleId(), c.GetOpId(), c.GetInstanceId())
	subTopics, found := mc.subscribers[subId]
	if !found {
		log.Errorf("not found subscription id %s", subId)
		return
	}

	for _, tpc := range subTopics.Topics {
		if sub, found := mc.topicSubscriptions[tpc]; found {
			for index, consumer := range sub.topicConsumers {
				if strings.EqualFold(subId, consumer.ConsumerId) {
					sub.topicConsumers = append(sub.topicConsumers[:index], sub.topicConsumers[index+1:]...)
					log.Infof("unsubscription topic %s for reqId %s, total subs %d", tpc, subId, len(sub.topicConsumers))
				}
			}
			if 0 == len(sub.topicConsumers) {
				delete(mc.topicSubscriptions, tpc)
				log.Infof("delete subscription for topic %s", tpc)
				mc.cli.conn.Unsubscribe(tpc)
			}
		}
	}
	delete(mc.subscribers, subId)
}

func (mc *mqttClientWrapper) Release(c api.StreamContext) {
	mc.unsubscribe(c)

	clients.ClientRegistry.Lock.Lock()
	mc.DeRef(c)
	clients.ClientRegistry.Lock.Unlock()
}

func (mc *mqttClientWrapper) SetConnectionSelector(conSelector string) {
	mc.conSelector = conSelector
}

func (mc *mqttClientWrapper) AddRef() {
	mc.refLock.Lock()
	defer mc.refLock.Unlock()
	mc.refCnt = mc.refCnt + 1
	conf.Log.Infof("mqtt client wrapper add refence for connection selector %s total refcount %d", mc.conSelector, mc.refCnt)
}

func (mc *mqttClientWrapper) DeRef(c api.StreamContext) {
	log := c.GetLogger()
	mc.refLock.Lock()
	defer mc.refLock.Unlock()

	mc.refCnt = mc.refCnt - 1
	if mc.refCnt == 0 {
		log.Infof("mqtt client wrapper reference count 0")
		if mc.conSelector != "" {
			conf.Log.Infof("remove mqtt client wrapper for connection selector %s", mc.conSelector)
			delete(clients.ClientRegistry.ShareClientStore, mc.conSelector)
		}
		_ = mc.cli.Disconnect()
	}
}
