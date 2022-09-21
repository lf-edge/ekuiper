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

package store

import (
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/memory/pubsub"
	"regexp"
)

// Reg registers a topic to save it to memory store
// Create a new go routine to listen to the topic and save the data to memory
func Reg(topic string, topicRegex *regexp.Regexp, keys []string) (*Table, error) {
	t, isNew := db.addTable(topic, keys)
	if isNew {
		go runTable(topic, topicRegex, t)
	}
	return t, nil
}

// runTable should only run in a single instance.
// This go routine is used to accumulate data in memory
// If the go routine close, the go routine exits but the data will be kept until table dropped
func runTable(topic string, topicRegex *regexp.Regexp, t *Table) {
	conf.Log.Infof("runTable %s", topic)
	ch := pubsub.CreateSub(topic, topicRegex, fmt.Sprintf("store_%s", topic), 1024)
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	for {
		select {
		case v, opened := <-ch:
			if !opened { // exit go routine is not sync with drop table
				return
			}
			t.add(v)
			conf.Log.Debugf("receive data %v for %s", v, topic)
		case <-ctx.Done():
			return
		}
	}
}

// Unreg unregisters a topic to remove it from memory store
func Unreg(topic string) error {
	// Must be an atomic operation
	return db.dropTable(topic)
}
