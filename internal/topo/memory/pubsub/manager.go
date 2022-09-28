// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"regexp"
	"sync"
)

const IdProperty = "topic"

type pubConsumers struct {
	count     int
	consumers map[string]chan api.SourceTuple // The consumer channel list [sourceId]chan
}

type subChan struct {
	regex *regexp.Regexp
	ch    chan api.SourceTuple
}

var (
	pubTopics = make(map[string]*pubConsumers)
	subExps   = make(map[string]*subChan)
	mu        = sync.RWMutex{}
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
		consumers: make(map[string]chan api.SourceTuple),
	}
	pubTopics[topic] = c
	for sourceId, sc := range subExps {
		if sc.regex.MatchString(topic) {
			addPubConsumer(topic, sourceId, sc.ch)
		}
	}
}

func CreateSub(wildcard string, regex *regexp.Regexp, sourceId string, bufferLength int) chan api.SourceTuple {
	mu.Lock()
	defer mu.Unlock()
	ch := make(chan api.SourceTuple, bufferLength)
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

func Produce(ctx api.StreamContext, topic string, data map[string]interface{}) {
	doProduce(ctx, topic, api.NewDefaultSourceTuple(data, map[string]interface{}{"topic": topic}))
}

func ProduceUpdatable(ctx api.StreamContext, topic string, data map[string]interface{}, rowkind string, keyval interface{}) {
	doProduce(ctx, topic, &UpdatableTuple{
		DefaultSourceTuple: api.NewDefaultSourceTuple(data, map[string]interface{}{"topic": topic}),
		Rowkind:            rowkind,
		Keyval:             keyval,
	})
}

func doProduce(ctx api.StreamContext, topic string, data api.SourceTuple) {
	c, exists := pubTopics[topic]
	if !exists {
		return
	}
	logger := ctx.GetLogger()
	mu.RLock()
	defer mu.RUnlock()
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

func ProduceError(ctx api.StreamContext, topic string, err error) {
	c, exists := pubTopics[topic]
	if !exists {
		return
	}
	logger := ctx.GetLogger()
	mu.RLock()
	defer mu.RUnlock()
	// broadcast to all consumers
	for name, out := range c.consumers {
		select {
		case out <- &xsql.ErrorSourceTuple{Error: err}:
			logger.Debugf("memory source broadcast error from topic %s to %s done", topic, name)
		case <-ctx.Done():
			// rule stop so stop waiting
		default:
			logger.Errorf("memory source topic %s drop message to %s", topic, name)
		}
	}

}

func addPubConsumer(topic string, sourceId string, ch chan api.SourceTuple) {
	var sinkConsumerChannels *pubConsumers
	if c, exists := pubTopics[topic]; exists {
		sinkConsumerChannels = c
	} else {
		sinkConsumerChannels = &pubConsumers{
			consumers: make(map[string]chan api.SourceTuple),
		}
		pubTopics[topic] = sinkConsumerChannels
	}
	if _, exists := sinkConsumerChannels.consumers[sourceId]; exists {
		conf.Log.Warnf("create memory source consumer for %s which is already exists", sourceId)
	} else {
		sinkConsumerChannels.consumers[sourceId] = ch
	}
}

func removePubConsumer(topic string, sourceId string, c *pubConsumers) {
	if _, exists := c.consumers[sourceId]; exists {
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
