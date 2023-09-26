// Copyright 2023-2023 emy120115@gmail.com
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
	"fmt"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"

	"github.com/lf-edge/ekuiper/internal/topo/context"
)

const (
	DefaultChannel = "TestChannel"
)

var data = [][]byte{
	[]byte("{\"timestamp\": 1646125996000, \"node_name\": \"node1\", \"group_name\": \"group1\", \"values\": {\"tag_name1\": 11.22, \"tag_name2\": \"yellow\"}, \"errors\": {\"tag_name3\": 122}}"),
	[]byte(`{"timestamp": 1646125996000, "node_name": "node1", "group_name": "group1", "values": {"tag_name1": 11.22, "tag_name2": "green","tag_name3":60}, "errors": {}}`),
	[]byte(`{"timestamp": 1646125996000, "node_name": "node1", "group_name": "group1", "values": {"tag_name1": 15.4, "tag_name2": "green","tag_name3":70}, "errors": {}}`),
}

var (
	addr string
	port string
	mr   *miniredis.Miniredis
)

func init() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	port = s.Port()
	addr = "localhost:" + port
	mr = s
}

func mockRedisPubSub(pub bool, sub bool, channel string) (*redis.Client, chan []byte) {
	var (
		client    *redis.Client
		subscribe *redis.PubSub
		ch        chan []byte
	)
	ctx := context.Background()
	client = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
	})
	subscribe = client.Subscribe(ctx, channel)

	if sub {
		ch = make(chan []byte)
		go func() {
			for {
				message, err := subscribe.ReceiveMessage(ctx)
				if err != nil {
					return
				}
				fmt.Printf("Redis RECEIVED: \"%s\"\n", message.Payload)
				ch <- []byte(message.Payload)
				fmt.Println("Redis Sent out")
			}
		}()
	}
	if pub {
		go func() {
			var msg []byte
			for _, msg = range data {
				fmt.Printf("Redis Publish: \"%s\"\n", string(msg))
				client.Publish(ctx, channel, msg)
			}
		}()
	}
	return client, ch
}
