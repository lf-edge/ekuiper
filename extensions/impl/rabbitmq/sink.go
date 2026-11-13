// Copyright 2026 EMQ Technologies Co., Ltd.
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

package rabbitmq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type sinkConf struct {
	Exchange         string            `json:"exchange"`
	RoutingKey       string            `json:"routingKey"`
	ContentType      string            `json:"contentType"`
	Persistent       bool              `json:"persistent"`
	Headers          map[string]string `json:"headers"`
	PublisherConfirm bool              `json:"publisherConfirm"`
}

func getSinkConf(props map[string]any) (*sinkConf, error) {
	c := &sinkConf{}
	if err := cast.MapToStruct(props, c); err != nil {
		return nil, fmt.Errorf("read properties fail with error: %v", err)
	}
	// The default exchange routes the message to the queue named by the routing key
	if c.Exchange == "" && c.RoutingKey == "" {
		return nil, errors.New("routingKey is required when publishing to the default exchange")
	}
	return c, nil
}

type sink struct {
	cc  *connConf
	sc  *sinkConf
	sch api.StatusChangeHandler

	// mu guards the connection and the status report, which is also accessed when the server closes the channel
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
	// bo and retryAt throttle the reconnection when the server is unavailable
	bo      *backoff.ExponentialBackOff
	retryAt time.Time
}

func (s *sink) Provision(ctx api.StreamContext, configs map[string]any) error {
	cc, err := getConnConf(ctx, configs)
	if err != nil {
		return err
	}
	sc, err := getSinkConf(configs)
	if err != nil {
		return err
	}
	s.cc = cc
	s.sc = sc
	s.bo = connection.NewExponentialBackOffWithMaxElapsedTime(0)
	return nil
}

// Connect tries to connect to the server. If it fails, the error is reported by the status and Collect reconnects.
func (s *sink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	s.sch = sch
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.connect(ctx); err != nil {
		ctx.GetLogger().Errorf("rabbitmq sink connect failed, will retry: %v", err)
	}
	return nil
}

// channel returns the open channel. It reconnects if the channel is closed and the backoff elapses.
func (s *sink) channel(ctx api.StreamContext) (*amqp.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch != nil && !s.ch.IsClosed() {
		return s.ch, nil
	}
	if s.ch != nil {
		_ = closeConn(s.conn, s.ch)
		s.conn, s.ch = nil, nil
	}
	if time.Now().Before(s.retryAt) {
		return nil, errorx.NewIOErr("rabbitmq sink is not connected")
	}
	if err := s.connect(ctx); err != nil {
		return nil, errorx.NewIOErr(err.Error())
	}
	return s.ch, nil
}

// connect must be called with mu locked
func (s *sink) connect(ctx api.StreamContext) error {
	err := s.doConnect(ctx)
	if err != nil {
		s.retryAt = time.Now().Add(s.bo.NextBackOff())
		s.sch(api.ConnectionDisconnected, err.Error())
		return err
	}
	s.bo.Reset()
	s.sch(api.ConnectionConnected, "")
	return nil
}

func (s *sink) doConnect(ctx api.StreamContext) error {
	conn, ch, err := s.cc.dial(ctx)
	if err != nil {
		return err
	}
	if s.sc.PublisherConfirm {
		if err := ch.Confirm(false); err != nil {
			_ = closeConn(conn, ch)
			return fmt.Errorf("enable publisher confirms failed: %v", err)
		}
	}
	// Report the disconnection once it happens instead of waiting for the next message
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		e := <-closeCh
		if e == nil {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		// Do not override the status if Collect has already replaced the channel
		if s.ch != ch {
			return
		}
		ctx.GetLogger().Errorf("rabbitmq sink disconnected: %v", e)
		s.sch(api.ConnectionDisconnected, e.Error())
	}()
	s.conn, s.ch = conn, ch
	ctx.GetLogger().Infof("rabbitmq sink connected to %s", s.cc.addr)
	return nil
}

func (s *sink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	ch, err := s.channel(ctx)
	if err != nil {
		return err
	}
	routingKey := s.sc.RoutingKey
	headers := s.sc.Headers
	// If the props support dynamic props(template), planner will guarantee the result has the parsed dynamic props
	if dp, ok := item.(api.HasDynamicProps); ok {
		if v, transformed := dp.DynamicProps(routingKey); transformed {
			routingKey = v
		}
		if len(headers) > 0 {
			newHeaders := make(map[string]string, len(headers))
			for k, v := range headers {
				if nv, transformed := dp.DynamicProps(v); transformed {
					v = nv
				}
				newHeaders[k] = v
			}
			headers = newHeaders
		}
	}
	msg := amqp.Publishing{
		ContentType: s.sc.ContentType,
		Timestamp:   timex.GetNow(),
		Body:        item.Raw(),
	}
	if s.sc.Persistent {
		msg.DeliveryMode = amqp.Persistent
	}
	if len(headers) > 0 {
		msg.Headers = make(amqp.Table, len(headers))
		for k, v := range headers {
			msg.Headers[k] = v
		}
	}
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, s.sc.Exchange, routingKey, false, false, msg)
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf("rabbitmq sink fails to publish to exchange %q with routing key %q: %v", s.sc.Exchange, routingKey, err))
	}
	// dc is nil if publisher confirms are disabled
	if dc != nil {
		acked, err := dc.WaitContext(ctx)
		if err != nil {
			return errorx.NewIOErr(fmt.Sprintf("rabbitmq sink fails to wait for the publisher confirm: %v", err))
		}
		if !acked {
			return errorx.NewIOErr(fmt.Sprintf("rabbitmq sink publishes to exchange %q with routing key %q but the server does not confirm it", s.sc.Exchange, routingKey))
		}
	}
	return nil
}

func (s *sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("closing rabbitmq sink")
	s.mu.Lock()
	defer s.mu.Unlock()
	err := closeConn(s.conn, s.ch)
	s.conn, s.ch = nil, nil
	return err
}

func (s *sink) Ping(ctx api.StreamContext, props map[string]any) error {
	return ping(ctx, props, nil)
}

func GetSink() api.Sink {
	return &sink{}
}

var (
	_ api.BytesCollector = &sink{}
	_ util.PingableConn  = &sink{}
)
