// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package testx

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
)

// errstring returns the string representation of an error.
func Errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func InitEnv(id string) {
	conf.InitConf()
	conf.TestId = id
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		conf.Log.Fatal(err)
	}
	err = store.SetupDefault(dataDir)
	if err != nil {
		conf.Log.Fatal(err)
	}
}

func InitBroker(id string) (string, func(), error) {
	// Create the new MQTT Server.
	server := mqtt.New(nil)
	// Allow all connections.
	_ = server.AddHook(new(auth.AllowHook), nil)

	// Create a TCP listener on a standard port.
	tcp := listeners.NewTCP(listeners.Config{ID: id, Address: ":0"})
	err := server.AddListener(tcp)
	if err != nil {
		return "", nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		<-ctx.Done()
		server.Close()
		wg.Done()
	}()
	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()
	return tcp.Address(), func() {
		cancel()
		// wait server close
		wg.Wait()
	}, nil
}

func TestHttp(client *http.Client, url string, method string) error {
	r, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	resp, err := client.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status code is not 200 for %s, code:%v", url, resp.StatusCode)
	}
	return nil
}

var body = []byte(`{
        "title": "Post title",
        "body": "Post description",
        "userId": 1
    }`)

func CreateWebsocketClient(ip string, port int, endpoint string) (*websocket.Conn, error) {
	u := url.URL{Scheme: "ws", Host: fmt.Sprintf("%s:%d", ip, port), Path: endpoint}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	return conn, err
}

type MockTuple struct {
	Map      map[string]any
	Template map[string]string
}

func (m MockTuple) DynamicProps(template string) (string, bool) {
	r, ok := m.Template[template]
	return r, ok
}

func (m MockTuple) AllProps() map[string]string {
	return m.Template
}

func (m MockTuple) Value(key, table string) (any, bool) {
	v, ok := m.Map[key]
	return v, ok
}

func (m MockTuple) ToMap() map[string]any {
	return m.Map
}

type MockRawTuple struct {
	Content  []byte
	Template map[string]string
}

func (m *MockRawTuple) DynamicProps(template string) (string, bool) {
	r, ok := m.Template[template]
	return r, ok
}

func (m *MockRawTuple) AllProps() map[string]string {
	return m.Template
}

func (m *MockRawTuple) Raw() []byte {
	return m.Content
}

func (m *MockRawTuple) Replace(newContent []byte) {
	m.Content = newContent
}
