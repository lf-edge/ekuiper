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

package mqtt

import (
	"fmt"
	"sync"

	"github.com/lf-edge/ekuiper/pkg/api"
)

var (
	connectionPool = make(map[string]*Connection)
	lock           sync.RWMutex
)

func GetConnection(ctx api.StreamContext, props map[string]any) (*Connection, error) {
	var clientId string
	if sid, ok := props["connectionSelector"]; ok {
		if s, ok := sid.(string); ok {
			clientId = s
		} else {
			return nil, fmt.Errorf("connectionSelector value: %v is not string", sid)
		}
	}
	if clientId == "" {
		return CreateClient(ctx, "", props)
	}
	lock.Lock()
	defer lock.Unlock()
	if conn, ok := connectionPool[clientId]; ok {
		conn.attach()
		return conn, nil
	} else {
		cli, err := CreateClient(ctx, clientId, props)
		if err != nil {
			return nil, err
		}
		connectionPool[clientId] = cli
		return cli, nil
	}
}

func DetachConnection(clientId string, topic string) {
	lock.Lock()
	defer lock.Unlock()
	if conn, ok := connectionPool[clientId]; ok {
		closed := conn.detach(topic)
		if closed {
			delete(connectionPool, clientId)
		}
	}
}
