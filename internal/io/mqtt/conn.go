// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"sync/atomic"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt/client"
	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt/v4client"
	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt/v5client"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type topicSubscription struct {
	Qos     byte
	Handler client.MessageHandler
	Refs    map[string]struct{}
}

type Connection struct {
	mu syncx.Mutex
	sm syncx.RWMutex
	client.Client
	id        string
	server    string
	connected atomic.Bool
	status    atomic.Value
	scHandler api.StatusChangeHandler
	// key is the topic. Each topic maintains one physical subscription and a ref set of logical subscribers.
	subscriptions map[string]*topicSubscription
}

func CreateConnection(_ api.StreamContext) modules.Connection {
	return &Connection{
		subscriptions: make(map[string]*topicSubscription),
	}
}

func ValidateConfig(props map[string]any) error {
	c := &client.CommonConfig{PVersion: "3.1.1"}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return err
	}
	ctx := mockContext.NewMockContext("1", "2")
	switch c.PVersion {
	case "3.1", "3.1.1", "4":
		_, err = v4client.ValidateConfig(ctx, props)
	case "5":
		_, err = v5client.ValidateConfig(ctx, props)
	default:
		return fmt.Errorf("unsupported protocol version %s", c.PVersion)
	}
	return err
}

func (conn *Connection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	c := &client.CommonConfig{PVersion: "3.1.1"}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return err
	}
	switch c.PVersion {
	case "3.1", "3.1.1", "4":
		conn.Client, err = v4client.Provision(ctx, props, conn.onConnect, conn.onConnectLost, conn.onReconnecting)
	case "5":
		conn.Client, err = v5client.Provision(ctx, props, conn.onConnect, conn.onConnectLost, conn.onReconnecting)
	default:
		return fmt.Errorf("unsupported protocol version %s", c.PVersion)
	}
	if err != nil {
		return err
	}
	conn.server = c.Server
	conn.status.Store(modules.ConnectionStatus{Status: api.ConnectionConnecting})
	conn.id = conId
	return nil
}

func (conn *Connection) GetId(_ api.StreamContext) string {
	return conn.id
}

func (conn *Connection) Dial(ctx api.StreamContext) error {
	err := conn.Client.Connect(ctx)
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf("found error when connecting for %s: %s", conn.server, err))
	}
	// store connected status immediately to avoid publish error due to onConnect is called slower
	conn.connected.Store(true)
	ctx.GetLogger().Infof("new mqtt client created")
	return nil
}

func (conn *Connection) Status(_ api.StreamContext) modules.ConnectionStatus {
	return conn.status.Load().(modules.ConnectionStatus)
}

func (conn *Connection) SetStatusChangeHandler(ctx api.StreamContext, sch api.StatusChangeHandler) {
	st := conn.status.Load().(modules.ConnectionStatus)
	sch(st.Status, st.ErrMsg)
	conn.mu.Lock()
	conn.scHandler = sch
	conn.mu.Unlock()
	ctx.GetLogger().Infof("trigger status change handler")
}

func (conn *Connection) onConnect(ctx api.StreamContext) {
	conn.connected.Store(true)
	conn.status.Store(modules.ConnectionStatus{Status: api.ConnectionConnected})
	conn.mu.Lock()
	handler := conn.scHandler
	conn.mu.Unlock()
	if handler != nil {
		handler(api.ConnectionConnected, "")
	} else {
		ctx.GetLogger().Warnf("sc handler has not set yet")
	}
	ctx.GetLogger().Infof("The connection to mqtt broker is established")
	for topic, info := range conn.getPhysicalSubscriptions() {
		err := conn.subscribePhysical(ctx, topic, info.Qos, info.Handler)
		if err != nil { // should never happen. If happens because of connection, it will retry later
			ctx.GetLogger().Errorf("Failed to subscribe topic %s: %v", topic, err)
		}
	}
}

func (conn *Connection) onConnectLost(ctx api.StreamContext, err error) {
	conn.connected.Store(false)
	conn.status.Store(modules.ConnectionStatus{Status: api.ConnectionDisconnected, ErrMsg: err.Error()})
	conn.mu.Lock()
	handler := conn.scHandler
	conn.mu.Unlock()
	if handler != nil {
		handler(api.ConnectionDisconnected, err.Error())
	}
	ctx.GetLogger().Infof("%v", err)
}

func (conn *Connection) onReconnecting(ctx api.StreamContext) {
	conn.status.Store(modules.ConnectionStatus{Status: api.ConnectionConnecting})
	conn.mu.Lock()
	handler := conn.scHandler
	conn.mu.Unlock()
	if handler != nil {
		handler(api.ConnectionConnecting, "")
	}
	ctx.GetLogger().Debugf("Reconnecting to mqtt broker")
}

func (conn *Connection) DetachSub(ctx api.StreamContext, props map[string]any) {
	topic, err := getTopicFromProps(props)
	if err != nil {
		ctx.GetLogger().Warnf("cannot find topic to unsub: %v", props)
		return
	}
	refID := getSubscriberID(ctx)
	shouldUnsubscribe := false
	conn.sm.Lock()
	if info, ok := conn.subscriptions[topic]; ok {
		delete(info.Refs, refID)
		if len(info.Refs) == 0 {
			delete(conn.subscriptions, topic)
			shouldUnsubscribe = true
		}
	}
	conn.sm.Unlock()
	if !shouldUnsubscribe {
		return
	}
	if conn.Client != nil {
		err = conn.Client.Unsubscribe(ctx, topic)
		if err != nil {
			ctx.GetLogger().Warnf("unsubscribe to topic %s: %v", topic, err)
		}
	}
}

func (conn *Connection) Close(ctx api.StreamContext) error {
	if conn == nil || conn.Client == nil {
		return nil
	}
	conn.Client.Disconnect(ctx)
	return nil
}

func (conn *Connection) Ping(ctx api.StreamContext) error {
	if conn.connected.Load() {
		return nil
	}
	return conn.Dial(ctx)
}

// MQTT features

func (conn *Connection) Publish(ctx api.StreamContext, topic string, qos byte, retained bool, payload []byte, properties map[string]string) error {
	// Need to return error immediately so that we can enable cache immediately
	if conn == nil || !conn.connected.Load() {
		return errorx.NewIOErr("mqtt client is not connected")
	}
	err := conn.Client.Publish(ctx, topic, qos, retained, payload, properties)
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf("publish to mqtt broker failed: %s", err))
	}
	return nil
}

func (conn *Connection) Subscribe(ctx api.StreamContext, topic string, qos byte, callback client.MessageHandler) error {
	refID := getSubscriberID(ctx)
	conn.sm.Lock()
	if info, ok := conn.subscriptions[topic]; ok {
		if info.Qos != qos {
			conn.sm.Unlock()
			return fmt.Errorf("topic %s already subscribed with qos %d, cannot subscribe with qos %d", topic, info.Qos, qos)
		}
		info.Refs[refID] = struct{}{}
		conn.sm.Unlock()
		return nil
	}
	conn.subscriptions[topic] = &topicSubscription{
		Qos:     qos,
		Handler: callback,
		Refs: map[string]struct{}{
			refID: {},
		},
	}
	conn.sm.Unlock()
	err := conn.subscribePhysical(ctx, topic, qos, callback)
	if err != nil {
		conn.sm.Lock()
		if info, ok := conn.subscriptions[topic]; ok && info.Qos == qos {
			delete(info.Refs, refID)
			if len(info.Refs) == 0 {
				delete(conn.subscriptions, topic)
			}
		}
		conn.sm.Unlock()
	}
	return err
}

func (conn *Connection) ParseMsg(ctx api.StreamContext, msg any) ([]byte, map[string]any, map[string]string) {
	return conn.Client.ParseMsg(ctx, msg)
}

const (
	dataSourceProp = "datasource"
)

func getTopicFromProps(props map[string]any) (string, error) {
	v, ok := props[dataSourceProp]
	if ok {
		return v.(string), nil
	}
	return "", fmt.Errorf("topic or datasource not defined")
}

func getSubscriberID(ctx api.StreamContext) string {
	return fmt.Sprintf("%s/%s/%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
}

func (conn *Connection) subscribePhysical(ctx api.StreamContext, topic string, qos byte, callback client.MessageHandler) error {
	return conn.Client.Subscribe(ctx, topic, qos, callback)
}

func (conn *Connection) getPhysicalSubscriptions() map[string]*client.SubscriptionInfo {
	conn.sm.RLock()
	defer conn.sm.RUnlock()
	snapshot := make(map[string]*client.SubscriptionInfo, len(conn.subscriptions))
	for topic, info := range conn.subscriptions {
		if len(info.Refs) == 0 {
			continue
		}
		snapshot[topic] = &client.SubscriptionInfo{
			Qos:     info.Qos,
			Handler: info.Handler,
		}
	}
	return snapshot
}

var _ modules.StatefulDialer = &Connection{}
