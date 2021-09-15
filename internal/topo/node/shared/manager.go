// Copyright 2021 INTECH Process Automation Ltd.
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

package shared

import (
	"fmt"
	"sync"
)

const IdProperty = "id"

type channels struct {
	id        string
	consumers map[string]chan map[string]interface{}
}

var sinkChannels = make(map[string]*channels)
var mu = sync.Mutex{}

func GetSink(props map[string]interface{}) (*sink, error) {
	id, err := getId(props)
	if err != nil {
		return nil, err
	}
	ch, err := getOrCreateSinkChannels(id)
	if err != nil {
		return nil, err
	}
	s := &sink{
		id: id,
		ch: ch,
	}
	return s, nil
}

func GetSource() *source {
	return &source{}
}

func getOrCreateSinkChannels(sink string) (*channels, error) {
	mu.Lock()
	defer mu.Unlock()

	if c, exists := sinkChannels[sink]; exists {
		return c, nil
	}
	c := createChannels(sink)
	sinkChannels[sink] = c
	return c, nil
}

func getOrCreateSinkConsumerChannel(sink string, source string) (chan map[string]interface{}, error) {
	mu.Lock()
	defer mu.Unlock()
	var sinkConsumerChannels *channels
	if c, exists := sinkChannels[sink]; exists {
		sinkConsumerChannels = c
	} else {
		sinkConsumerChannels = createChannels(sink)
	}
	var ch chan map[string]interface{}
	if sourceChannel, exists := sinkConsumerChannels.consumers[source]; exists {
		ch = sourceChannel
	} else {
		ch = make(chan map[string]interface{})
		sinkConsumerChannels.consumers[source] = ch
	}
	return ch, nil
}

func getId(props map[string]interface{}) (string, error) {
	if t, ok := props[IdProperty]; ok {
		if id, casted := t.(string); casted {
			return id, nil
		}
		return "", fmt.Errorf("can't cast value %s to string", t)
	}
	return "", fmt.Errorf("there is no topic property in the memory action")
}

func closeSourceConsumerChannel(sink string, source string) error {
	mu.Lock()
	defer mu.Unlock()

	if sinkConsumerChannels, exists := sinkChannels[sink]; exists {
		if sourceChannel, exists := sinkConsumerChannels.consumers[source]; exists {
			close(sourceChannel)
			delete(sinkConsumerChannels.consumers, source)
		}
	}
	return nil
}

func closeSink(sink string) error {
	mu.Lock()
	defer mu.Unlock()

	if sinkConsumerChannels, exists := sinkChannels[sink]; exists {
		for s, c := range sinkConsumerChannels.consumers {
			close(c)
			delete(sinkConsumerChannels.consumers, s)
		}
	}
	delete(sinkChannels, sink)
	return nil
}

func createChannels(sink string) *channels {
	return &channels{
		id:        sink,
		consumers: make(map[string]chan map[string]interface{}),
	}
}
