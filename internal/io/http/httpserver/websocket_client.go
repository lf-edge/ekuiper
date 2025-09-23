// Copyright 2024-2024 EMQ Technologies Co., Ltd.
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

package httpserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
)

type WebsocketClient struct {
	RecvTopic string
	SendTopic string

	scheme    string
	addr      string
	path      string
	tlsConfig *tls.Config
	conn      *websocket.Conn
	wg        *sync.WaitGroup
	cancel    context.CancelFunc
}

func NewWebsocketClient(scheme, addr, path string, tlsConfig *tls.Config) *WebsocketClient {
	if scheme == "" {
		scheme = "ws"
	}
	return &WebsocketClient{
		scheme:    scheme,
		addr:      addr,
		path:      path,
		tlsConfig: tlsConfig,
		wg:        &sync.WaitGroup{},
	}
}

func (c *WebsocketClient) Connect() error {
	d := &websocket.Dialer{
		HandshakeTimeout: 3 * time.Second,
		TLSClientConfig:  c.tlsConfig,
	}
	if len(c.addr) < 1 {
		return fmt.Errorf("addr should be defined")
	}
	u := url.URL{Scheme: c.scheme, Host: c.addr, Path: c.path}
	conn, _, err := d.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *WebsocketClient) Run(ctx api.StreamContext) (string, string) {
	c.RecvTopic = recvTopic(c.path, false)
	c.SendTopic = sendTopic(c.path, false)
	pubsub.CreatePub(c.RecvTopic)
	c.handleProcess(ctx)
	return c.RecvTopic, c.SendTopic
}

func (c *WebsocketClient) handleProcess(parCtx api.StreamContext) {
	ctx, cancel := parCtx.WithCancel()
	c.cancel = cancel
	c.wg.Add(2)
	go recvProcess(ctx, c.RecvTopic, c.conn, cancel, c.wg)
	go sendProcess(ctx, c.SendTopic, "", c.conn, cancel, c.wg)
}

func (c *WebsocketClient) Close(ctx api.StreamContext) error {
	pubsub.RemovePub(c.RecvTopic)
	c.cancel()
	c.wg.Wait()
	return nil
}
