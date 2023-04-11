// Copyright 2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build edgex
// +build edgex

package edgex

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/pkg/api"
	"strings"
	"sync"
)

type messageHandler func(stopChan chan struct{}, msgChan chan types.MessageEnvelope)

type edgexSubscriptionInfo struct {
	topic          string
	handler        messageHandler
	stop           chan struct{}
	topicConsumers []*clients.ConsumerInfo
	hasError       bool
}

type edgexClientWrapper struct {
	cli *EdgexClient

	subLock sync.RWMutex
	//topic: subscriber
	//multiple go routine can sub same topic
	topicSubscriptions map[string]*edgexSubscriptionInfo

	//consumerId: SubscribedTopics
	subscribers map[string]clients.SubscribedTopics

	conSelector string

	refLock sync.RWMutex
	refCnt  uint64
}

func NewEdgeClientWrapper(props map[string]interface{}) (clients.ClientWrapper, error) {
	if props == nil {
		conf.Log.Warnf("props is nill for mqtt client wrapper")
	}
	client := &EdgexClient{}
	err := client.CfgValidate(props)
	if err != nil {
		return nil, err
	}

	cliWpr := &edgexClientWrapper{
		cli:                client,
		subLock:            sync.RWMutex{},
		topicSubscriptions: make(map[string]*edgexSubscriptionInfo),
		subscribers:        make(map[string]clients.SubscribedTopics),
		refCnt:             1,
	}

	err = client.Connect()
	if err != nil {
		return nil, err
	}
	return cliWpr, nil
}

func (mc *edgexClientWrapper) Publish(c api.StreamContext, topic string, message []byte, params map[string]interface{}) error {
	env := types.NewMessageEnvelope(message, c)

	env.ContentType = "application/json"
	if pk, ok := params["contentType"]; ok {
		if v, ok := pk.(string); ok {
			env.ContentType = v
		}
	}
	err := mc.cli.Publish(env, topic)
	if err != nil {
		return err
	}
	return nil
}

func (mc *edgexClientWrapper) newMessageHandler(topic string, sub *edgexSubscriptionInfo, messageErrors chan error) func(stopChan chan struct{}, msgChan chan types.MessageEnvelope) {
	return func(stopChan chan struct{}, msgChan chan types.MessageEnvelope) {
		for {
			select {
			case <-stopChan:
				conf.Log.Infof("message handler for topic %s stopped", topic)
				return
			case msgErr := <-messageErrors:
				//broadcast to all topic subscribers only one time
				if sub != nil && !sub.hasError {
					for _, consumer := range sub.topicConsumers {
						select {
						case consumer.SubErrors <- msgErr:
							break
						default:
							conf.Log.Warnf("consumer SubErrors channel full for request id %s", consumer.ConsumerId)
						}
					}
					sub.hasError = true
				}
			case msg, ok := <-msgChan:
				if !ok {
					for _, consumer := range sub.topicConsumers {
						close(consumer.ConsumerChan)
					}
					conf.Log.Errorf("message handler for topic %s stopped", topic)
					return
				}
				//broadcast to all topic subscribers
				if sub != nil {
					if sub.hasError == true {
						sub.hasError = false
						conf.Log.Infof("Subscription to edgex messagebus topic %s recoverd", topic)
					}
					for _, consumer := range sub.topicConsumers {
						select {
						case consumer.ConsumerChan <- &msg:
							break
						default:
							conf.Log.Warnf("consumer chan full for request id %s", consumer.ConsumerId)
						}
					}
				}
			}
		}
	}
}

func (mc *edgexClientWrapper) Subscribe(c api.StreamContext, subChan []api.TopicChannel, messageErrors chan error, _ map[string]interface{}) error {
	log := c.GetLogger()

	mc.subLock.Lock()
	defer mc.subLock.Unlock()

	subId := fmt.Sprintf("%s_%s_%d", c.GetRuleId(), c.GetOpId(), c.GetInstanceId())
	if _, ok := mc.subscribers[subId]; ok {
		return fmt.Errorf("already have subscription %s", subId)
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
			sub := &edgexSubscriptionInfo{
				topic: tpc,
				stop:  make(chan struct{}, 1),
				topicConsumers: []*clients.ConsumerInfo{
					{
						ConsumerId:   subId,
						ConsumerChan: tpChan.Messages,
						SubErrors:    messageErrors,
					},
				},
				hasError: false,
			}
			log.Infof("new subscription for topic %s, reqId is %s", tpc, subId)
			message := make(chan types.MessageEnvelope)
			errChan := make(chan error)

			if err := mc.cli.Subscribe(message, tpc, errChan); err != nil {
				return err
			}
			sub.handler = mc.newMessageHandler(tpc, sub, errChan)
			go sub.handler(sub.stop, message)

			mc.topicSubscriptions[tpc] = sub
		}
	}
	mc.subscribers[subId] = subTopics

	return nil
}

func (mc *edgexClientWrapper) unsubscribe(c api.StreamContext) {
	log := c.GetLogger()
	mc.subLock.Lock()
	defer mc.subLock.Unlock()

	subId := fmt.Sprintf("%s_%s_%d", c.GetRuleId(), c.GetOpId(), c.GetInstanceId())
	subTopics, found := mc.subscribers[subId]
	if !found {
		log.Errorf("not found subscription id %s", subId)
		return
	}
	// just clean the consumers, do not clean the topic subscription
	for _, tpc := range subTopics.Topics {
		if sub, found := mc.topicSubscriptions[tpc]; found {
			for index, consumer := range sub.topicConsumers {
				if strings.EqualFold(subId, consumer.ConsumerId) {
					sub.topicConsumers = append(sub.topicConsumers[:index], sub.topicConsumers[index+1:]...)
					log.Infof("unsubscription topic %s for reqId %s, total subs %d", tpc, subId, len(sub.topicConsumers))
				}
			}
		}
	}
	delete(mc.subscribers, subId)
}

func (mc *edgexClientWrapper) SetConnectionSelector(conSelector string) {
	mc.conSelector = conSelector
}

func (mc *edgexClientWrapper) GetConnectionSelector() string {
	return mc.conSelector
}

func (mc *edgexClientWrapper) Release(c api.StreamContext) bool {
	mc.unsubscribe(c)

	return mc.deRef(c)
}

func (mc *edgexClientWrapper) AddRef() {
	mc.refLock.Lock()
	defer mc.refLock.Unlock()

	mc.refCnt = mc.refCnt + 1
	conf.Log.Infof("edgex client wrapper add refence for connection selector %s total refcount %d", mc.conSelector, mc.refCnt)
}

func (mc *edgexClientWrapper) deRef(c api.StreamContext) bool {
	log := c.GetLogger()
	mc.refLock.Lock()
	defer mc.refLock.Unlock()

	mc.refCnt = mc.refCnt - 1
	if mc.refCnt != 0 {
		conf.Log.Infof("edgex client wrapper derefence for connection selector %s total refcount %d", mc.conSelector, mc.refCnt)
	}
	if mc.refCnt == 0 {
		log.Infof("mqtt client wrapper reference count 0")
		// clean the go routine that waiting on the messages
		for _, sub := range mc.topicSubscriptions {
			sub.stop <- struct{}{}
		}
		_ = mc.cli.Disconnect()
		return true
	} else {
		return false
	}
}
