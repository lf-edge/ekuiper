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
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestConnConf(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")
	tests := []struct {
		name  string
		props map[string]any
		addr  string
		err   string
	}{
		{
			name:  "missing server",
			props: map[string]any{},
			err:   "server is required",
		},
		{
			name:  "invalid scheme",
			props: map[string]any{"server": "http://user:secret@localhost:5672/"},
			err:   "invalid server: AMQP scheme must be either 'amqp://' or 'amqps://'",
		},
		{
			name:  "password without username",
			props: map[string]any{"server": "amqp://localhost/", "password": "secret"},
			err:   "username is required when password is set",
		},
		{
			name:  "tls without amqps",
			props: map[string]any{"server": "amqp://localhost/", "insecureSkipVerify": true},
			err:   "TLS properties are only supported by amqps:// server",
		},
		{
			name:  "default port",
			props: map[string]any{"server": "amqp://user:secret@localhost/vhost"},
			addr:  "amqp://localhost:5672",
		},
		{
			name:  "tls",
			props: map[string]any{"server": "amqps://localhost/", "insecureSkipVerify": true, "username": "user", "password": "secret"},
			addr:  "amqps://localhost:5671",
		},
		{
			name:  "ipv6",
			props: map[string]any{"server": "amqp://[::1]:5673"},
			addr:  "amqp://[::1]:5673",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := getConnConf(ctx, tt.props)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.addr, c.addr)
			if v, ok := tt.props["insecureSkipVerify"]; ok {
				require.NotNil(t, c.tlsConfig)
				require.Equal(t, v, c.tlsConfig.InsecureSkipVerify)
			} else {
				require.Nil(t, c.tlsConfig)
			}
		})
	}
}

// closedServer returns the URI of a server that refuses the connection
func closedServer(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return fmt.Sprintf("amqp://user:secret@%s/", addr)
}

func TestPingFail(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")
	props := map[string]any{"server": closedServer(t), "datasource": "q", "routingKey": "q"}
	err := GetSource().(*source).Ping(ctx, props)
	require.ErrorContains(t, err, "connect to amqp://127.0.0.1:")
	require.NotContains(t, err.Error(), "secret")
	err = GetSink().(*sink).Ping(ctx, props)
	require.ErrorContains(t, err, "connect to amqp://127.0.0.1:")
	require.NotContains(t, err.Error(), "secret")
	require.EqualError(t, GetSink().(*sink).Ping(ctx, map[string]any{}), "server is required")
}

// statusRecorder records the connection status changes
type statusRecorder struct {
	sync.Mutex
	statuses []string
	messages []string
}

func (r *statusRecorder) handle(status string, message string) {
	r.Lock()
	defer r.Unlock()
	r.statuses = append(r.statuses, status)
	r.messages = append(r.messages, message)
}

func (r *statusRecorder) last() (string, string, int) {
	r.Lock()
	defer r.Unlock()
	if len(r.statuses) == 0 {
		return "", "", 0
	}
	return r.statuses[len(r.statuses)-1], r.messages[len(r.messages)-1], len(r.statuses)
}
