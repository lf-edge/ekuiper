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

//go:build msgpack

package service

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kconf "github.com/lf-edge/ekuiper/v2/internal/conf"
)

// Regression for GHSA-26rj-rmp3-wpfp: the msgpack executor must reject
// loopback destinations when private networking is disabled, and a rejected
// dial must release the executor lock so the next invocation is not stuck.
func TestMsgpackExecutorBlocksLoopbackWhenPrivateNetDisabled(t *testing.T) {
	orig := kconf.Config.Basic.EnablePrivateNet
	kconf.Config.Basic.EnablePrivateNet = false
	defer func() { kconf.Config.Basic.EnablePrivateNet = orig }()
	u, err := url.Parse("tcp://127.0.0.1:50000")
	require.NoError(t, err)
	exe := &msgpackExecutor{
		interfaceOpt: &interfaceOpt{addr: u, timeout: 2 * time.Second},
	}
	invoke := func() error {
		_, err := exe.InvokeFunction(nil, "SayHello", nil)
		return err
	}
	err = invoke()
	require.Error(t, err)
	require.Contains(t, err.Error(), "in internal network")
	// A second invocation must return, not block forever on a leaked mutex.
	done := make(chan error, 1)
	go func() { done <- invoke() }()
	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "in internal network")
	case <-time.After(10 * time.Second):
		t.Fatal("second invocation blocked: executor lock not released on dial error")
	}
}
