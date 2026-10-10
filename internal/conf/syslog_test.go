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

//go:build !windows

package conf

import (
	"bytes"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf/logger"
)

func TestSetConsoleAndFileLogDisabledKeepsSyslog(t *testing.T) {
	address := filepath.Join(t.TempDir(), "syslog.sock")
	listener, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: address, Net: "unixgram"})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	originalOutput := Log.Out
	originalHooks := Log.ReplaceHooks(make(logrus.LevelHooks))
	t.Cleanup(func() {
		Log.SetOutput(originalOutput)
		Log.ReplaceHooks(originalHooks)
	})

	var output bytes.Buffer
	Log.SetOutput(&output)
	require.NoError(t, SetConsoleAndFileLog(false, false))
	require.NoError(t, logger.InitSyslog("unixgram", address, "info", "kuiper"))
	const message = "syslog remains enabled with console and file disabled"
	Log.Error(message)

	require.NoError(t, listener.SetReadDeadline(time.Now().Add(time.Second)))
	buffer := make([]byte, 4096)
	n, err := listener.Read(buffer)
	require.NoError(t, err)
	require.Contains(t, string(buffer[:n]), message)
	require.Empty(t, output.String())
}
