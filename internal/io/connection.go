// Copyright 2024 EMQ Technologies Co., Ltd.
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

package io

import "github.com/lf-edge/ekuiper/contract/v2/api"

type ConnStatus int

const (
	Connecting ConnStatus = iota
	Connected
	Disconnect
	Subscribed
	SubError
)

type MessageClient interface {
	Subscribe(c api.StreamContext, subChan []TopicChannel, messageErrors chan error, params map[string]interface{}) error
	Publish(c api.StreamContext, topic string, message []byte, params map[string]interface{}) error
	Ping() error
}

// TopicChannel is the data structure for subscriber
type TopicChannel struct {
	// Topic for subscriber to filter on if any
	Topic string
	// Messages is the returned message channel for the subscriber
	Messages chan<- interface{}
}
