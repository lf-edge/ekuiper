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
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"
)

var (
	connectionPool = make(map[string]*Connection)
	lock           sync.RWMutex
)

func GetConnection(ctx api.StreamContext, selId string, props map[string]any) (*Connection, error) {
	if selId == "" {
		return CreateClient(ctx, "", props)
	}
	lock.Lock()
	defer lock.Unlock()
	if conn, ok := connectionPool[selId]; ok {
		conn.attach()
		return conn, nil
	} else {
		cli, err := CreateClient(ctx, selId, props)
		if err != nil {
			return nil, err
		}
		connectionPool[selId] = cli
		return cli, nil
	}
}

func DetachConnection(conn *Connection, selId string, subscribedTopic string) {
	var closed bool
	if subscribedTopic != "" {
		closed = conn.detachSub(subscribedTopic)
	} else {
		closed = conn.detachPub()
	}
	lock.Lock()
	defer lock.Unlock()
	if _, ok := connectionPool[selId]; closed && ok {
		delete(connectionPool, selId)
	}
}
