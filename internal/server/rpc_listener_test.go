// Copyright 2025 EMQ Technologies Co., Ltd.
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

//go:build rpc || !core

package server

import (
	"net"
	"net/rpc"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestRPCListenerSwitch(t *testing.T) {
	old := conf.Config
	t.Cleanup(func() { conf.Config = old })
	conf.Config = &model.KuiperConf{}
	conf.Config.Basic.Ip = "127.0.0.1"
	conf.Config.Basic.AllowExternalFileAccess = true

	// An occupied port must not matter while RPC is disabled.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	conf.Config.Basic.Port = listener.Addr().(*net.TCPAddr).Port
	conf.Config.Basic.DisableRpc = true
	disabled := &rpcComp{}
	disabled.serve()
	require.Nil(t, disabled.s)
	disabled.close()

	require.NoError(t, listener.Close())
	conf.Config.Basic.DisableRpc = false
	enabled := &rpcComp{}
	enabled.serve()
	t.Cleanup(enabled.close)
	require.NotNil(t, enabled.s)
	require.Equal(t, listener.Addr().String(), enabled.s.Addr)
	var client *rpc.Client
	require.Eventually(t, func() bool {
		client, err = rpc.DialHTTP("tcp", enabled.s.Addr)
		return err == nil
	}, 3*time.Second, 10*time.Millisecond)
	defer client.Close()
	var reply string
	// A real method invocation must reach the server and report the missing file.
	err = client.Call("Server.Import", filepath.Join(t.TempDir(), "missing.json"), &reply)
	require.ErrorContains(t, err, "fail to read file")
}
