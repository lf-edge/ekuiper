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

package v5client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session/state"
	storefile "github.com/eclipse/paho.golang/paho/store/file"
	"github.com/google/uuid"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt/client"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type Client struct {
	cm *autopaho.ConnectionManager
	sync.Mutex
	// subscription route info
	router paho.Router
	// record if already have subscription for a topic
	subs                map[string]struct{}
	EnableClientSession bool
}

type ConnectionConfig struct {
	Server                       string `json:"server"`
	ClientId                     string `json:"clientid"`
	Uname                        string `json:"username"`
	Password                     string `json:"password"`
	EnableClientSession          bool   `json:"enableClientSession"`
	ClientStatePath              string `json:"clientStatePath"`
	SessionExpiryIntervalSeconds int    `json:"sessionExpiryIntervalSeconds"`
	serverUrl                    *url.URL
	tls                          *tls.Config
}

func Provision(ctx api.StreamContext, props map[string]any, onConnect client.ConnectHandler, onConnectLost client.ConnectErrorHandler, _ client.ConnectHandler) (*Client, error) {
	cc, err := ValidateConfig(ctx, props)
	if err != nil {
		return nil, err
	}
	r := paho.NewStandardRouter()
	cli := &Client{
		router: r,
		subs:   make(map[string]struct{}),
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{cc.serverUrl},
		// TODO backoff?
		ConnectRetryDelay: time.Second,
		KeepAlive:         20, // Keepalive message should be sent every 20 seconds
		// CleanStartOnInitialConnection defaults to false. Setting this to true will clear the session on the first connection.
		CleanStartOnInitialConnection: !cc.EnableClientSession,
		// SessionExpiryInterval - Seconds that a session will survive after disconnection.
		// It is important to set this because otherwise, any queued messages will be lost if the connection drops and
		// the server will not queue messages while it is down. The specific setting will depend upon your needs
		// (60 = 1 minute, 3600 = 1 hour, 86400 = one day, 0xFFFFFFFE = 136 years, 0xFFFFFFFF = don't expire)
		SessionExpiryInterval: uint32(cc.SessionExpiryIntervalSeconds),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			onConnect(ctx)
		},
		OnConnectError: func(err error) {
			onConnectLost(ctx, err)
		},
		// eclipse/paho.golang/paho provides base mqtt functionality, the below config will be passed in for each connection
		ClientConfig: paho.ClientConfig{
			// If you are using QOS 1/2, then it's important to specify a client id (which must be unique)
			ClientID: cc.ClientId,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					ctx.GetLogger().Debugf("received message on topic %s; body: %s (retain: %t)", pr.Packet.Topic, pr.Packet.Payload, pr.Packet.Retain)
					r.Route(pr.Packet.Packet())
					return true, nil
				},
			},
			OnClientError: func(err error) { ctx.GetLogger().Warnf("client error: %s", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					ctx.GetLogger().Infof("server requested disconnect: %s", d.Properties.ReasonString)
				} else {
					ctx.GetLogger().Infof("server requested disconnect; reason code: %d", d.ReasonCode)
				}
			},
		},
	}
	if cc.EnableClientSession {
		cliState, err := storefile.New(cc.ClientStatePath, fmt.Sprintf("%v_%v_cli_", ctx.GetRuleId(), ctx.GetOpId()), ".pkt")
		if err != nil {
			return nil, err
		}
		srvState, err := storefile.New(cc.ClientStatePath, fmt.Sprintf("%v_%v_srv_", ctx.GetRuleId(), ctx.GetOpId()), ".pkt")
		if err != nil {
			return nil, err
		}
		cliCfg.Session = state.New(cliState, srvState)
	}
	if cc.Uname != "" {
		cliCfg.ConnectUsername = cc.Uname
	}
	if cc.Password != "" {
		cliCfg.ConnectPassword = []byte(cc.Password)
	}
	cli.EnableClientSession = cc.EnableClientSession
	cm, err := autopaho.NewConnection(ctx, cliCfg) // starts process; will reconnect until context cancelled
	if err != nil {
		return nil, err
	}
	cli.cm = cm
	return cli, nil
}

func (c *Client) Connect(ctx api.StreamContext) error {
	if err := c.cm.AwaitConnection(ctx); err != nil {
		return errorx.NewIOErr(fmt.Sprintf("found error when connecting mqtt: %s", err))
	}
	return nil
}

func (c *Client) Subscribe(ctx api.StreamContext, topic string, qos byte, callback client.MessageHandler) error {
	topics := strings.Split(topic, ",")
	c.Lock()
	defer c.Unlock()
	s := &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{},
	}
	// register router first
	for _, subTopic := range topics {
		if _, alreadySub := c.subs[subTopic]; !alreadySub {
			c.subs[subTopic] = struct{}{}
			c.router.RegisterHandler(subTopic, func(p *paho.Publish) {
				callback(ctx, p)
			})
			s.Subscriptions = append(s.Subscriptions, paho.SubscribeOptions{
				Topic: subTopic, QoS: qos,
			})
		}
	}
	suback, err := c.cm.Subscribe(ctx, s)
	if err != nil {
		if suback != nil {
			if suback.Properties != nil {
				return fmt.Errorf("suscribe to %s error: %s", topic, suback.Properties.ReasonString)
			} else {
				return fmt.Errorf("suscribe to %s error: %s", topic, suback.Reasons)
			}
		}
		return err
	}
	return nil
}

func (c *Client) Publish(ctx api.StreamContext, topic string, qos byte, retained bool, payload []byte, properties map[string]string) error {
	msg := &paho.Publish{
		QoS:     qos,
		Topic:   topic,
		Retain:  retained,
		Payload: payload,
	}
	if properties != nil && len(properties) > 0 {
		props := make([]paho.UserProperty, 0, len(properties))
		for k, v := range properties {
			props = append(props, paho.UserProperty{
				Key:   k,
				Value: v,
			})
		}
		msg.Properties = &paho.PublishProperties{
			User: props,
		}
	}
	resp, err := c.cm.Publish(ctx, msg)
	if err != nil {
		if resp != nil {
			if resp.Properties != nil {
				return fmt.Errorf("publish error %s: %v", resp.Properties.ReasonString, err)
			} else {
				return fmt.Errorf("publish error %d: %v", resp.ReasonCode, err)
			}
		}
		return err
	} else {
		return nil
	}
}

func (c *Client) Unsubscribe(ctx api.StreamContext, topic string) error {
	c.Lock()
	defer c.Unlock()
	topics := strings.Split(topic, ",")
	for _, subTopic := range topics {
		delete(c.subs, subTopic)
		if !c.EnableClientSession {
			unsuback, err := c.cm.Unsubscribe(ctx, &paho.Unsubscribe{
				Topics: []string{subTopic},
			})
			c.router.UnregisterHandler(subTopic)
			// Do not exit immediately when unsub error. Just remove unsub handler
			if err != nil {
				if unsuback != nil {
					if unsuback.Properties != nil {
						return fmt.Errorf("unsuscribe to %s error: %s", topic, unsuback.Properties.ReasonString)
					} else {
						return fmt.Errorf("unsuscribe to %s error: %s", topic, unsuback.Reasons)
					}
				}
				return err
			}
		}
	}
	return nil
}

func (c *Client) Disconnect(ctx api.StreamContext) {
	dctx, dcancel := context.WithTimeout(context.Background(), time.Second*5)
	defer dcancel()
	err := c.cm.Disconnect(dctx)
	if err != nil {
		ctx.GetLogger().Warnf("disconnect error: %s", err)
	}
}

func (c *Client) ParseMsg(ctx api.StreamContext, msg any) ([]byte, map[string]any, map[string]string) {
	if packet, ok := msg.(*paho.Publish); ok {
		meta := map[string]any{
			"topic":     packet.Topic,
			"qos":       packet.QoS,
			"messageId": packet.PacketID,
		}
		var properties map[string]string
		if packet.Properties != nil && len(packet.Properties.User) > 0 {
			properties = make(map[string]string, len(packet.Properties.User))
			for _, prop := range packet.Properties.User {
				properties[prop.Key] = prop.Value
			}
		}
		return packet.Payload, meta, properties
	} else {
		ctx.GetLogger().Errorf("receive invalid msg %v", msg)
	}
	return nil, nil, nil
}

func ValidateConfig(ctx api.StreamContext, props map[string]any) (*ConnectionConfig, error) {
	c := &ConnectionConfig{
		SessionExpiryIntervalSeconds: 60,
	}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, err
	}

	if c.Server == "" {
		return nil, fmt.Errorf("missing server property")
	}
	u, err := url.Parse(c.Server)
	if err != nil {
		return nil, err
	}
	c.serverUrl = u

	if c.ClientId == "" {
		c.ClientId = uuid.New().String()
	}
	tlsConfig, err := cert.GenTLSConfig(ctx, props)
	if err != nil {
		return nil, err
	}
	c.tls = tlsConfig
	if c.EnableClientSession && len(c.ClientStatePath) == 0 {
		return nil, errors.New("missing client state path")
	}
	return c, nil
}

var _ client.Client = &Client{}
