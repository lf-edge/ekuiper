// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package pubsub

import (
	"regexp"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

const IdProperty = "topic"

type pubConsumers struct {
	count             int
	consumers         map[string]chan any // The consumer channel list [sourceId]chan, the value must be message or message list
	consumersReplaced map[string]int
}

type subChan struct {
	regex *regexp.Regexp
	ch    chan any
}

var (
	pubTopics = make(map[string]*pubConsumers)
	subExps   = make(map[string]*subChan)
	mu        = syncx.RWMutex{}
)

func CreatePub(topic string) {
	mu.Lock()
	defer mu.Unlock()

	if c, exists := pubTopics[topic]; exists {
		c.count += 1
		return
	}
	c := &pubConsumers{
		count:     1,
		consumers: make(map[string]chan any),
	}
	pubTopics[topic] = c
	for sourceId, sc := range subExps {
		if sc.regex.MatchString(topic) {
			addPubConsumer(topic, sourceId, sc.ch)
		}
	}
}

func CreateSub(wildcard string, regex *regexp.Regexp, sourceId string, bufferLength int) chan any {
	mu.Lock()
	defer mu.Unlock()
	ch := make(chan any, bufferLength)
	if regex != nil {
		subExps[sourceId] = &subChan{
			regex: regex,
			ch:    ch,
		}
		for topic := range pubTopics {
			if regex.MatchString(topic) {
				addPubConsumer(topic, sourceId, ch)
			}
		}
	} else {
		addPubConsumer(wildcard, sourceId, ch)
	}
	return ch
}

func CloseSourceConsumerChannel(topic string, sourceId string) {
	mu.Lock()
	defer mu.Unlock()

	if sc, exists := subExps[sourceId]; exists {
		close(sc.ch)
		delete(subExps, sourceId)
		for _, c := range pubTopics {
			removePubConsumer(topic, sourceId, c)
		}
	} else {
		if sinkConsumerChannels, exists := pubTopics[topic]; exists {
			removePubConsumer(topic, sourceId, sinkConsumerChannels)
		}
	}
}

func RemovePub(topic string) {
	mu.Lock()
	defer mu.Unlock()

	if sinkConsumerChannels, exists := pubTopics[topic]; exists {
		sinkConsumerChannels.count -= 1
		if len(sinkConsumerChannels.consumers) == 0 && sinkConsumerChannels.count == 0 {
			delete(pubTopics, topic)
		}
	}
}

func ProduceAny(ctx api.StreamContext, topic string, data any) {
	doProduce(ctx, topic, data)
}

func Produce(ctx api.StreamContext, topic string, data MemTuple) {
	doProduce(ctx, topic, data)
}

func ProduceList(ctx api.StreamContext, topic string, list []MemTuple) {
	doProduce(ctx, topic, list)
}

func ProduceError(ctx api.StreamContext, topic string, err error) {
	doProduce(ctx, topic, err)
}

func doProduce(ctx api.StreamContext, topic string, data any) {
	mu.RLock()
	defer mu.RUnlock()
	c, exists := pubTopics[topic]
	if !exists {
		return
	}
	logger := ctx.GetLogger()
	// broadcast to all consumers
	for name, out := range c.consumers {
		select {
		case out <- data:
			logger.Debugf("memory source broadcast from topic %s to %s done", topic, name)
		case <-ctx.Done():
			// rule stop so stop waiting
		default:
			logger.Errorf("memory source topic %s drop message to %s", topic, name)
		}
	}
}

func addPubConsumer(topic string, sourceId string, ch chan any) {
	var sinkConsumerChannels *pubConsumers
	if c, exists := pubTopics[topic]; exists {
		sinkConsumerChannels = c
	} else {
		sinkConsumerChannels = &pubConsumers{
			consumers: make(map[string]chan any),
		}
		pubTopics[topic] = sinkConsumerChannels
	}
	if _, exists := sinkConsumerChannels.consumers[sourceId]; exists {
		conf.Log.Warnf("create memory source consumer for %s which is already exists", sourceId)
		// If already exist, it is usually the rule is restarting and the previous handle is not released yet
		// Just use the latest ch as the handle. Also record the replaced status so that it won't remove all handles during removal of the previous handle
		if sinkConsumerChannels.consumersReplaced == nil {
			sinkConsumerChannels.consumersReplaced = map[string]int{
				sourceId: 1,
			}
		} else {
			if v, ok := sinkConsumerChannels.consumersReplaced[sourceId]; ok {
				sinkConsumerChannels.consumersReplaced[sourceId] = v + 1
			} else {
				sinkConsumerChannels.consumersReplaced[sourceId] = 1
			}
		}
	}
	sinkConsumerChannels.consumers[sourceId] = ch
}

func removePubConsumer(topic string, sourceId string, c *pubConsumers) {
	if _, exists := c.consumers[sourceId]; exists {
		if c.consumersReplaced != nil {
			if v, ok := c.consumersReplaced[sourceId]; ok && v > 0 {
				c.consumersReplaced[sourceId] = v - 1
				conf.Log.Warnf("remove memory source consumer for %s late than creating new one", sourceId)
				return
			}
		}
		delete(c.consumers, sourceId)
	}
	if len(c.consumers) == 0 && c.count == 0 {
		delete(pubTopics, topic)
	}
}

// Reset For testing only
func Reset() {
	pubTopics = make(map[string]*pubConsumers)
	subExps = make(map[string]*subChan)
}
