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

package clients

import (
	"github.com/lf-edge/ekuiper/pkg/api"
)

type ConsumerInfo struct {
	ConsumerId   string
	ConsumerChan chan<- interface{}
	SubErrors    chan error
}

type SubscribedTopics struct {
	Topics []string
}

type ClientFactoryFunc func(props map[string]interface{}) (ClientWrapper, error)

type ClientWrapper interface {
	Subscribe(c api.StreamContext, subChan []api.TopicChannel, messageErrors chan error, params map[string]interface{}) error
	Release(c api.StreamContext) bool
	Publish(c api.StreamContext, topic string, message []byte, params map[string]interface{}) error
	SetConnectionSelector(conSelector string)
	GetConnectionSelector() string
	AddRef()
}
