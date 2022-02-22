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
	"github.com/lf-edge/ekuiper/internal/topo/connection/types"
	defaultCtx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"strings"
	"sync"
)

type mqttCtxKey int

const (
	_ mqttCtxKey = iota
	ctxKeyMqttRequest
)

type RequestInfo struct {
	Qos      byte
	Retained bool
}

// WithRequestInfo creates a new context that has MqttRequestInfo injected.
func WithRequestInfo(parent *defaultCtx.DefaultContext, reqInfo *RequestInfo) *defaultCtx.DefaultContext {
	return defaultCtx.WithValue(parent, ctxKeyMqttRequest, reqInfo)
}

// GetRequestInfo tries to retrieve MqttRequestInfo from the given context.
// If it doesn't exist, an nil is returned.
func GetRequestInfo(parent *defaultCtx.DefaultContext) *RequestInfo {
	if reqInfo := parent.Value(ctxKeyMqttRequest); reqInfo != nil {
		return reqInfo.(*RequestInfo)
	}
	return nil
}

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

	refLock sync.RWMutex
	refCnt  uint64
}

func NewMqttClientWrapper(props map[string]interface{}) (types.ClientWrapper, error) {
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

	err = client.Connect(cliWpr.onConnectHandler)
	if err != nil {
		return nil, err
	}

	return cliWpr, nil
}

func (mc *mqttClientWrapper) onConnectHandler(_ pahoMqtt.Client) {
	// activeSubscriptions will be empty on the first connection.
	// On a re-connect is when the subscriptions must be re-created.
	conf.Log.Infof("The connection to mqtt broker %s client id %s established", mc.cli.srv, mc.cli.clientid)
	mc.subLock.RLock()
	defer mc.subLock.RUnlock()
	for topic, subscription := range mc.topicSubscriptions {
		token := mc.cli.conn.Subscribe(topic, subscription.qos, subscription.topicHandler)
		if token.Error() != nil {
			for _, con := range subscription.topicConsumers {
				con.SubErrors <- token.Error()
			}
		}
	}
}

func (mc *mqttClientWrapper) newMessageHandler(ctx api.StreamContext, sub *mqttSubscriptionInfo) pahoMqtt.MessageHandler {
	return func(client pahoMqtt.Client, message pahoMqtt.Message) {
		if sub != nil {
			for _, consumer := range sub.topicConsumers {
				go func(c *clients.ConsumerInfo) {
					select {
					case c.ConsumerChan <- &types.MessageEnvelope{MqttMsg: message}:
						break
					case <-ctx.Done():
						break
					}
				}(consumer)
			}
		}
	}
}

func (mc *mqttClientWrapper) Publish(c api.StreamContext, topic string, message []byte) error {
	reqInfo := GetRequestInfo(c.(*defaultCtx.DefaultContext))
	if reqInfo == nil {
		return fmt.Errorf("not find reqInfo for mqtt subscription %s_%s_%d", c.GetRuleId(), c.GetOpId(), c.GetInstanceId())
	}
	err := mc.cli.Publish(topic, reqInfo.Qos, reqInfo.Retained, message)
	if err != nil {
		return err
	}

	return nil
}

func (mc *mqttClientWrapper) Subscribe(c api.StreamContext, subChan []types.TopicChannel, messageErrors chan error) error {
	log := c.GetLogger()

	mc.subLock.Lock()
	defer mc.subLock.Unlock()

	subId := fmt.Sprintf("%s_%s_%d", c.GetRuleId(), c.GetOpId(), c.GetInstanceId())
	if _, ok := mc.subscribers[subId]; ok {
		return fmt.Errorf("already have subscription %s", subId)
	}
	reqInfo := GetRequestInfo(c.(*defaultCtx.DefaultContext))
	if reqInfo == nil {
		return fmt.Errorf("not find reqInfo for mqtt subscription %s", subId)
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
				qos:   reqInfo.Qos,
				topicConsumers: []*clients.ConsumerInfo{
					{
						ConsumerId:   subId,
						ConsumerChan: tpChan.Messages,
						SubErrors:    messageErrors,
					},
				},
			}
			sub.topicHandler = mc.newMessageHandler(c, sub)
			log.Infof("new subscription for topic %s, reqId is %s", tpc, subId)
			token := mc.cli.conn.Subscribe(tpc, reqInfo.Qos, sub.topicHandler)
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
		return
	}

	for _, tpc := range subTopics.Topics {
		if sub, found := mc.topicSubscriptions[tpc]; found {
			for index, consumer := range sub.topicConsumers {
				if strings.EqualFold(subId, consumer.ConsumerId) {
					close(consumer.ConsumerChan)
					sub.topicConsumers = append(sub.topicConsumers[:index], sub.topicConsumers[index+1:]...)
					log.Infof("unsubscription topic %s for reqId %s, total subs %d", tpc, subId, len(sub.topicConsumers))
				}
			}
			if 0 == len(sub.topicConsumers) {
				delete(mc.topicSubscriptions, tpc)
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
