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
	"math"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type sourceConf struct {
	Queue         string `json:"datasource"`
	ConsumerTag   string `json:"consumerTag"`
	PrefetchCount int    `json:"prefetchCount"`
	AutoAck       bool   `json:"autoAck"`
}

func getSourceConf(props map[string]any) (*sourceConf, error) {
	c := &sourceConf{}
	if err := cast.MapToStruct(props, c); err != nil {
		return nil, fmt.Errorf("read properties fail with error: %v", err)
	}
	if c.Queue == "" {
		return nil, errors.New("datasource is required, it must be the name of the queue to consume")
	}
	if c.PrefetchCount < 0 || c.PrefetchCount > math.MaxUint16 {
		return nil, fmt.Errorf("prefetchCount must be in range 0-%d", math.MaxUint16)
	}
	return c, nil
}

type source struct {
	cc  *connConf
	sc  *sourceConf
	sch api.StatusChangeHandler

	conn       *amqp.Connection
	ch         *amqp.Channel
	deliveries <-chan amqp.Delivery
	closeCh    chan *amqp.Error
}

func (s *source) Provision(ctx api.StreamContext, configs map[string]any) error {
	cc, err := getConnConf(ctx, configs)
	if err != nil {
		return err
	}
	sc, err := getSourceConf(configs)
	if err != nil {
		return err
	}
	s.cc = cc
	s.sc = sc
	return nil
}

// Connect tries to consume the queue. If it fails, the error is reported by the status and Subscribe keeps retrying.
func (s *source) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	s.sch = sch
	if err := s.connect(ctx); err != nil {
		ctx.GetLogger().Errorf("rabbitmq source connect failed, will retry: %v", err)
		sch(api.ConnectionDisconnected, err.Error())
	}
	return nil
}

func (s *source) connect(ctx api.StreamContext) error {
	conn, ch, err := s.cc.dial(ctx)
	if err != nil {
		return err
	}
	if s.sc.PrefetchCount > 0 {
		if err := ch.Qos(s.sc.PrefetchCount, 0, false); err != nil {
			_ = closeConn(conn, ch)
			return fmt.Errorf("set prefetchCount failed: %v", err)
		}
	}
	// Buffered so that the client never blocks when sending the close reason
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	deliveries, err := ch.Consume(s.sc.Queue, s.sc.ConsumerTag, s.sc.AutoAck, false, false, false, nil)
	if err != nil {
		_ = closeConn(conn, ch)
		return fmt.Errorf("consume queue %s failed: %v", s.sc.Queue, err)
	}
	s.conn, s.ch, s.deliveries, s.closeCh = conn, ch, deliveries, closeCh
	ctx.GetLogger().Infof("rabbitmq source consumes queue %s from %s", s.sc.Queue, s.cc.addr)
	s.sch(api.ConnectionConnected, "")
	return nil
}

// Subscribe consumes the messages until the rule stops. If the connection or the channel is closed, it reconnects with backoff.
func (s *source) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, _ api.ErrorIngest) error {
	bo := connection.NewExponentialBackOffWithMaxElapsedTime(0)
	for {
		if s.deliveries != nil {
			err := s.consume(ctx, ingest)
			if err == nil {
				return nil
			}
			ctx.GetLogger().Errorf("rabbitmq source disconnected, will reconnect: %v", err)
			s.sch(api.ConnectionDisconnected, err.Error())
			s.disconnect()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(bo.NextBackOff()):
		}
		if err := s.connect(ctx); err != nil {
			ctx.GetLogger().Errorf("rabbitmq source reconnect failed: %v", err)
			s.sch(api.ConnectionDisconnected, err.Error())
			continue
		}
		bo.Reset()
	}
}

// consume ingests the deliveries. It returns nil when the rule stops, or the reason why the deliveries end.
func (s *source) consume(ctx api.StreamContext, ingest api.BytesIngest) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-s.deliveries:
			if !ok {
				// The client sends the close reason before closing the deliveries. If there is no reason, the channel is
				// still open but the server cancels the consumer, for example, because the queue is deleted.
				select {
				case e := <-s.closeCh:
					if e != nil {
						return e
					}
				default:
				}
				return fmt.Errorf("the consumer of queue %s is cancelled", s.sc.Queue)
			}
			ingest(ctx, d.Body, deliveryMeta(d), timex.GetNow())
			if !s.sc.AutoAck {
				if err := d.Ack(false); err != nil {
					ctx.GetLogger().Warnf("rabbitmq source fails to ack the message: %v", err)
				}
			}
		}
	}
}

func (s *source) disconnect() error {
	err := closeConn(s.conn, s.ch)
	s.conn, s.ch, s.deliveries, s.closeCh = nil, nil, nil, nil
	return err
}

func (s *source) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("closing rabbitmq source")
	return s.disconnect()
}

func (s *source) Ping(ctx api.StreamContext, props map[string]any) error {
	queue, _ := props["datasource"].(string)
	return ping(ctx, props, func(ch *amqp.Channel) error {
		if queue == "" {
			return nil
		}
		if _, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil); err != nil {
			return fmt.Errorf("check queue %s failed: %v", queue, err)
		}
		return nil
	})
}

// deliveryMeta exposes the properties of the message as the metadata, so it can be read by meta() in rules
func deliveryMeta(d amqp.Delivery) map[string]any {
	meta := map[string]any{
		"exchange":    d.Exchange,
		"routingKey":  d.RoutingKey,
		"redelivered": d.Redelivered,
	}
	for k, v := range map[string]string{
		"contentType":     d.ContentType,
		"contentEncoding": d.ContentEncoding,
		"correlationId":   d.CorrelationId,
		"replyTo":         d.ReplyTo,
		"messageId":       d.MessageId,
		"type":            d.Type,
		"appId":           d.AppId,
	} {
		if v != "" {
			meta[k] = v
		}
	}
	if !d.Timestamp.IsZero() {
		meta["timestamp"] = d.Timestamp.UnixMilli()
	}
	if len(d.Headers) > 0 {
		meta["headers"] = headerValue(d.Headers)
	}
	return meta
}

// headerValue converts the AMQP field value to the types used in the rules
func headerValue(v any) any {
	switch vt := v.(type) {
	case amqp.Table:
		m := make(map[string]any, len(vt))
		for k, e := range vt {
			m[k] = headerValue(e)
		}
		return m
	case []any:
		a := make([]any, len(vt))
		for i, e := range vt {
			a[i] = headerValue(e)
		}
		return a
	case []byte:
		return string(vt)
	case int8:
		return int64(vt)
	case uint8:
		return int64(vt)
	case int16:
		return int64(vt)
	case uint16:
		return int64(vt)
	case int32:
		return int64(vt)
	case uint32:
		return int64(vt)
	case float32:
		return float64(vt)
	case amqp.Decimal:
		return float64(vt.Value) / math.Pow10(int(vt.Scale))
	case time.Time:
		return vt.UnixMilli()
	default:
		return v
	}
}

func GetSource() api.Source {
	return &source{}
}

var (
	_ api.BytesSource   = &source{}
	_ util.PingableConn = &source{}
)
