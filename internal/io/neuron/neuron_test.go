// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package neuron

import (
	"fmt"
	"log"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pair"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/nng"
)

var data = [][]byte{
	[]byte("{\"timestamp\": 1646125996000, \"node_name\": \"node1\", \"group_name\": \"group1\", \"values\": {\"tag_name1\": 11.22, \"tag_name2\": \"yellow\"}, \"errors\": {\"tag_name3\": 122}}"),
	[]byte(`{"timestamp": 1646125996000, "node_name": "node1", "group_name": "group1", "values": {"tag_name1": 11.22, "tag_name2": "green","tag_name3":60}, "errors": {}}`),
	[]byte(`{"timestamp": 1646125996000, "node_name": "node1", "group_name": "group1", "values": {"tag_name1": 15.4, "tag_name2": "green","tag_name3":70}, "errors": {}}`),
}

func init() {
	connection.InitConnectionManager4Test()
	testx.InitEnv("neuron_tests")
	modules.RegisterConnection("nng", nng.CreateConnection)
}

// mockNeuron start the nng pair server
func mockNeuron(send bool, recv bool, url string) (mangos.Socket, chan []byte) {
	var (
		sock mangos.Socket
		err  error
		ch   chan []byte
	)
	if sock, err = pair.NewSocket(); err != nil {
		log.Fatalf("can't get new pair socket: %s", err)
	}
	if err = sock.Listen(url); err != nil {
		log.Fatalf("can't listen on pair socket: %s", err.Error())
	}
	log.Printf("listen on pair socket")
	if recv {
		ch = make(chan []byte)
		go func() {
			for {
				var msg []byte
				var err error
				if msg, err = sock.Recv(); err == nil {
					fmt.Printf("Neuron RECEIVED: \"%s\"\n", string(msg))
					ch <- msg
					fmt.Println("Neuron Sent out")
				}
			}
		}()
	}
	if send {
		go func() {
			for _, msg := range data {
				time.Sleep(10 * time.Millisecond)
				fmt.Printf("Neuron SENDING \"%s\"\n", msg)
				if err := sock.Send(msg); err != nil {
					fmt.Printf("failed sending: %s\n", err)
				}
			}
		}()
	}
	return sock, ch
}
