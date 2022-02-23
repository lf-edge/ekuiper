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

//go:build edgex
// +build edgex

package edgex

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	defaultCtx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"strings"
	"sync"
)

type edgexCtxKey int

const (
	_ edgexCtxKey = iota
	ctxKeyEdgeXRequest
)

type RequestInfo struct {
	ContentType string
}

// WithRequestInfo creates a new context that has MqttRequestInfo injected.
func WithRequestInfo(parent *defaultCtx.DefaultContext, reqInfo *RequestInfo) *defaultCtx.DefaultContext {
	return defaultCtx.WithValue(parent, ctxKeyEdgeXRequest, reqInfo)
}

// GetRequestInfo tries to retrieve MqttRequestInfo from the given context.
// If it doesn't exist, an nil is returned.
func GetRequestInfo(parent *defaultCtx.DefaultContext) *RequestInfo {
	if reqInfo := parent.Value(ctxKeyEdgeXRequest); reqInfo != nil {
		return reqInfo.(*RequestInfo)
	}
	return nil
}

type messageHandler func(stopChan chan struct{}, msgChan chan types.MessageEnvelope)

type edgexSubscriptionInfo struct {
	topic          string
	msgChan        chan api.MessageEnvelope
	handler        messageHandler
	stop           chan struct{}
	topicConsumers []*clients.ConsumerInfo
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

func (mc *edgexClientWrapper) Publish(c api.StreamContext, topic string, message []byte) error {
	env := types.NewMessageEnvelope(message, c)
	reqInfo := GetRequestInfo(c.(*defaultCtx.DefaultContext))
	if reqInfo == nil {
		return fmt.Errorf("not find reqInfo for mqtt subscription %s_%s_%d", c.GetRuleId(), c.GetOpId(), c.GetInstanceId())
	}
	env.ContentType = reqInfo.ContentType

	err := mc.cli.Publish(env, topic)
	if err != nil {
		return err
	}
	return nil
}

func (mc *edgexClientWrapper) messageHandler(topic string, sub *edgexSubscriptionInfo) func(stopChan chan struct{}, msgChan chan types.MessageEnvelope) {
	return func(stopChan chan struct{}, msgChan chan types.MessageEnvelope) {
		for {
			select {
			case <-stopChan:
				conf.Log.Infof("message handler for topic %s stopped", topic)
				return
			case msg, ok := <-msgChan:
				if !ok {
					for _, consumer := range sub.topicConsumers {
						close(consumer.ConsumerChan)
					}
					return
				}
				//broadcast to all topic subscribers
				if sub != nil {
					for _, consumer := range sub.topicConsumers {
						select {
						case consumer.ConsumerChan <- &api.MessageEnvelope{EdgexMsg: msg}:
							break
						default:
						}
					}
				}
			}
		}
	}
}

func (mc *edgexClientWrapper) Subscribe(c api.StreamContext, subChan []api.TopicChannel, messageErrors chan error) error {
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
			}
			log.Infof("new subscription for topic %s, reqId is %s", tpc, subId)
			message := make(chan types.MessageEnvelope)
			errChan := make(chan error)

			if err := mc.cli.Subscribe(message, tpc, errChan); err != nil {
				return err
			}
			sub.handler = mc.messageHandler(tpc, sub)
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
				sub.stop <- struct{}{}
			}
		}
	}
	delete(mc.subscribers, subId)
}

func (mc *edgexClientWrapper) SetConnectionSelector(conSelector string) {
	mc.conSelector = conSelector
}

func (mc *edgexClientWrapper) Release(c api.StreamContext) {
	mc.unsubscribe(c)

	clients.ClientRegistry.Lock.Lock()
	mc.DeRef(c)
	clients.ClientRegistry.Lock.Unlock()
}

func (mc *edgexClientWrapper) AddRef() {
	mc.refLock.Lock()
	defer mc.refLock.Unlock()

	mc.refCnt = mc.refCnt + 1
	conf.Log.Infof("edgex client wrapper add refence for connection selector %s total refcount %d", mc.conSelector, mc.refCnt)
}

func (mc *edgexClientWrapper) DeRef(c api.StreamContext) {
	log := c.GetLogger()
	mc.refLock.Lock()
	defer mc.refLock.Unlock()

	mc.refCnt = mc.refCnt - 1
	if mc.refCnt != 0 {
		conf.Log.Infof("edgex client wrapper derefence for connection selector %s total refcount %d", mc.conSelector, mc.refCnt)
	}
	if mc.refCnt == 0 {
		log.Infof("mqtt client wrapper reference count 0")
		if mc.conSelector != "" {
			conf.Log.Infof("remove mqtt client wrapper for connection selector %s", mc.conSelector)
			delete(clients.ClientRegistry.ShareClientStore, mc.conSelector)
		}
		_ = mc.cli.Disconnect()
	}

}
