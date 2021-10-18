// Copyright 2021 EMQ Technologies Co., Ltd.
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

package memory

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"regexp"
	"sync"
)

const IdProperty = "topic"

type pubConsumers struct {
	count     int
	consumers map[string]chan map[string]interface{} // The consumer channel list [sourceId]chan
}

type subChan struct {
	regex *regexp.Regexp
	ch    chan map[string]interface{}
}

var (
	pubTopics = make(map[string]*pubConsumers)
	subExps   = make(map[string]*subChan)
	mu        = sync.RWMutex{}
)

func GetSink() *sink {
	return &sink{}
}

func GetSource() *source {
	return &source{}
}

func createPub(topic string) {
	mu.Lock()
	defer mu.Unlock()

	if c, exists := pubTopics[topic]; exists {
		c.count += 1
		return
	}
	c := &pubConsumers{
		count:     1,
		consumers: make(map[string]chan map[string]interface{}),
	}
	pubTopics[topic] = c
	for sourceId, sc := range subExps {
		if sc.regex.MatchString(topic) {
			addPubConsumer(topic, sourceId, sc.ch)
		}
	}
}

func createSub(wildcard string, regex *regexp.Regexp, sourceId string) chan map[string]interface{} {
	mu.Lock()
	defer mu.Unlock()
	ch := make(chan map[string]interface{})
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

func closeSourceConsumerChannel(topic string, sourceId string) error {
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
	return nil
}

func closeSink(topic string) error {
	mu.Lock()
	defer mu.Unlock()

	if sinkConsumerChannels, exists := pubTopics[topic]; exists {
		sinkConsumerChannels.count -= 1
		if len(sinkConsumerChannels.consumers) == 0 && sinkConsumerChannels.count == 0 {
			delete(pubTopics, topic)
		}
	}
	return nil
}

func produce(ctx api.StreamContext, topic string, data map[string]interface{}) {
	c, exists := pubTopics[topic]
	if !exists {
		return
	}
	logger := ctx.GetLogger()
	var wg sync.WaitGroup
	mu.RLock()
	// blocking to retain the sequence, expect the source to consume the data immediately
	wg.Add(len(c.consumers))
	for n, out := range c.consumers {
		go func(name string, output chan<- map[string]interface{}) {
			select {
			case output <- data:
				logger.Debugf("broadcast from topic %s to %s done", topic, name)
			case <-ctx.Done():
				// rule stop so stop waiting
			}
			wg.Done()
		}(n, out)
	}
	mu.RUnlock()
	wg.Wait()
}

func addPubConsumer(topic string, sourceId string, ch chan map[string]interface{}) {
	var sinkConsumerChannels *pubConsumers
	if c, exists := pubTopics[topic]; exists {
		sinkConsumerChannels = c
	} else {
		sinkConsumerChannels = &pubConsumers{
			consumers: make(map[string]chan map[string]interface{}),
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
