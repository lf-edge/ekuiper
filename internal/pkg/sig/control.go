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

package sig

import (
	"context"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type MqttControl struct {
	// make sure this is never null
	cli      mqtt.Client
	interval time.Duration
	topic    string
	// signals
	lock syncx.RWMutex
	sigs map[string]struct{}
	// func to stop
	cancel context.CancelFunc
}

var Ctrl *MqttControl

const (
	CtrlTopic    = "ctrl/subready"
	CtrlAckTopic = "ctrl/suback"
)

// InitMQTTControl Should only called once
func InitMQTTControl() {
	Ctrl = NewMQTTControl("tcp://127.0.0.1:1883", "ek_ctrl")
}

func NewMQTTControl(server string, cid string) *MqttControl {
	// connect to MQTT
	conf.Log.Infof("connect to local broker for control channel")
	mc := &MqttControl{
		sigs:     make(map[string]struct{}),
		interval: time.Second,
		lock:     syncx.RWMutex{},
	}
	// Connect to MQTT
	opts := mqtt.NewClientOptions().AddBroker(server).SetProtocolVersion(4).SetClientID(cid).SetAutoReconnect(true).SetConnectRetry(true).SetConnectRetryInterval(100 * time.Millisecond).SetMaxReconnectInterval(1 * time.Second)
	opts.OnConnect = func(client mqtt.Client) {
		conf.Log.Infof("mqtt control channel connected")
		client.Subscribe(CtrlAckTopic, 0, func(client mqtt.Client, msg mqtt.Message) {
			mc.Rem(string(msg.Payload()))
		})
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		conf.Log.Errorf("mqtt control channel disconected for %v", err)
		// subscribe?
	}
	opts.OnReconnecting = func(client mqtt.Client, options *mqtt.ClientOptions) {
		conf.Log.Infof("mqtt control channel is reconnecting")
	}
	cli := mqtt.NewClient(opts)
	token := cli.Connect()
	go func() {
		err := handleToken(token)
		if err != nil {
			conf.Log.Warnf("found error when connecting for mqtt control channel: %s", err)
		}
	}()
	mc.cli = cli
	return mc
}

func handleToken(token mqtt.Token) error {
	if !token.WaitTimeout(5 * time.Second) {
		return errorx.NewIOErr("timeout")
	} else if token.Error() != nil {
		return errorx.NewIOErr(token.Error().Error())
	}
	return nil
}

func (c *MqttControl) Add(name string) {
	conf.Log.Infof("mqtt control add %s", name)
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.sigs) == 0 {
		if c.cancel != nil {
			c.cancel()
		}
		ctx, cancel := context.WithCancel(context.Background())
		c.cancel = cancel
		go c.run(ctx)
	}
	c.pub(name)
	c.sigs[name] = struct{}{}
}

func (c *MqttControl) Rem(name string) {
	conf.Log.Infof("mqtt control remove %s", name)
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.sigs, name)
	if len(c.sigs) == 0 && c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
}

// start run when there are topics and stop when no topics needed
func (c *MqttControl) run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	conf.Log.Infof("mqtt control channel loop started")
	for {
		select {
		case <-ticker.C:
			c.scan()
		case <-ctx.Done():
			conf.Log.Infof("mqtt control channel loop exit")
			return
		}
	}
}

func (c *MqttControl) scan() {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for k := range c.sigs {
		c.pub(k)
	}
}

func (c *MqttControl) pub(message string) {
	c.cli.Publish(CtrlTopic, 0, false, []byte(message))
	conf.Log.Debugf("mqtt control chan publish %s", message)
}
