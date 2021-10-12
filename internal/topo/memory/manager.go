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
	"github.com/lf-edge/ekuiper/pkg/api"
	"sync"
)

const IdProperty = "topic"

type channels struct {
	producerCount int
	consumers     map[string]chan map[string]interface{} // The consumer channel list [sourceId]chan
}

var topics = make(map[string]*channels)
var mu = sync.RWMutex{}

func GetSink() *sink {
	return &sink{}
}

func GetSource() *source {
	return &source{}
}

func getOrCreateSinkChannels(topic string) {
	mu.Lock()
	defer mu.Unlock()

	if c, exists := topics[topic]; exists {
		c.producerCount += 1
		return
	}
	c := &channels{
		producerCount: 1,
		consumers:     make(map[string]chan map[string]interface{}),
	}
	topics[topic] = c
}

func getOrCreateSinkConsumerChannel(sink string, source string) chan map[string]interface{} {
	mu.Lock()
	defer mu.Unlock()
	var sinkConsumerChannels *channels
	if c, exists := topics[sink]; exists {
		sinkConsumerChannels = c
	} else {
		sinkConsumerChannels = &channels{
			consumers: make(map[string]chan map[string]interface{}),
		}
	}
	var ch chan map[string]interface{}
	if _, exists := sinkConsumerChannels.consumers[source]; !exists {
		ch = make(chan map[string]interface{})
		sinkConsumerChannels.consumers[source] = ch
	}
	return ch
}

func closeSourceConsumerChannel(topic string, sourceId string) error {
	mu.Lock()
	defer mu.Unlock()

	if sinkConsumerChannels, exists := topics[topic]; exists {
		if sourceChannel, exists := sinkConsumerChannels.consumers[sourceId]; exists {
			close(sourceChannel)
			delete(sinkConsumerChannels.consumers, sourceId)
		}
		if len(sinkConsumerChannels.consumers) == 0 && sinkConsumerChannels.producerCount == 0 {
			delete(topics, topic)
		}
	}
	return nil
}

func closeSink(topic string) error {
	mu.Lock()
	defer mu.Unlock()

	if sinkConsumerChannels, exists := topics[topic]; exists {
		sinkConsumerChannels.producerCount -= 1
		if len(sinkConsumerChannels.consumers) == 0 && sinkConsumerChannels.producerCount == 0 {
			delete(topics, topic)
		}
	}
	return nil
}

func produce(ctx api.StreamContext, topic string, data map[string]interface{}) {
	c, exists := topics[topic]
	if !exists {
		return
	}
	logger := ctx.GetLogger()
	var wg sync.WaitGroup
	mu.RLock()
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
