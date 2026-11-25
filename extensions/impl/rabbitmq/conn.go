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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
)

// connConf is the connection configuration shared by the source and the sink.
type connConf struct {
	// Server is the AMQP URI such as amqp://localhost:5672/vhost
	Server   string `json:"server"`
	Username string `json:"username"`
	Password string `json:"password"`

	// addr is the server address without the credentials, used in logs and errors
	addr      string
	tlsConfig *tls.Config
}

func getConnConf(ctx api.StreamContext, props map[string]any) (*connConf, error) {
	c := &connConf{}
	if err := cast.MapToStruct(props, c); err != nil {
		return nil, fmt.Errorf("read properties fail with error: %v", err)
	}
	if c.Server == "" {
		return nil, errors.New("server is required")
	}
	// Do not print the server in the error message as it may contain the password
	uri, err := amqp.ParseURI(c.Server)
	if err != nil {
		return nil, fmt.Errorf("invalid server: %v", err)
	}
	c.addr = fmt.Sprintf("%s://%s", uri.Scheme, net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port)))
	if c.Username == "" && c.Password != "" {
		return nil, errors.New("username is required when password is set")
	}
	tlsConfig, err := cert.GenTLSConfig(ctx, props)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil && uri.Scheme != "amqps" {
		return nil, errors.New("TLS properties are only supported by amqps:// server")
	}
	c.tlsConfig = tlsConfig
	return c, nil
}

// dial connects to the server and opens a channel
func (c *connConf) dial(ctx api.StreamContext) (*amqp.Connection, *amqp.Channel, error) {
	cfg := amqp.Config{
		Properties: amqp.NewConnectionProperties(),
	}
	// Show which rule owns the connection in the RabbitMQ management UI
	cfg.Properties.SetClientConnectionName(fmt.Sprintf("ekuiper-%s-%s", ctx.GetRuleId(), ctx.GetOpId()))
	if c.tlsConfig != nil {
		// The client may modify the config, so pass a copy
		cfg.TLSClientConfig = c.tlsConfig.Clone()
	}
	if c.Username != "" {
		cfg.SASL = []amqp.Authentication{&amqp.PlainAuth{Username: c.Username, Password: c.Password}}
	}
	conn, err := amqp.DialConfig(c.Server, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to %s failed: %v", c.addr, err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("open channel to %s failed: %v", c.addr, err)
	}
	return conn, ch, nil
}

// closeConn closes the channel and the connection. They are safe to be closed again or be nil.
func closeConn(conn *amqp.Connection, ch *amqp.Channel) error {
	var errs []error
	if ch != nil && !ch.IsClosed() {
		errs = append(errs, ch.Close())
	}
	if conn != nil && !conn.IsClosed() {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

// ping checks the server is reachable with the configured credentials
func ping(ctx api.StreamContext, props map[string]any, check func(ch *amqp.Channel) error) error {
	c, err := getConnConf(ctx, props)
	if err != nil {
		return err
	}
	conn, ch, err := c.dial(ctx)
	if err != nil {
		return err
	}
	defer closeConn(conn, ch)
	if check != nil {
		return check(ch)
	}
	return nil
}
