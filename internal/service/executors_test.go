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

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kconf "github.com/lf-edge/ekuiper/v2/internal/conf"
)

// Regression for GHSA-26rj-rmp3-wpfp: the exact dialer wired into
// grpc.DialContext must reject loopback destinations when private networking
// is disabled. Tested directly because grpc.DialContext with WithBlock retries
// dial errors until the context deadline, which masks the policy error behind
// a generic timeout at the InvokeFunction level.
func TestGrpcExecutorDialerBlocksLoopbackWhenPrivateNetDisabled(t *testing.T) {
	orig := kconf.Config.Basic.EnablePrivateNet
	kconf.Config.Basic.EnablePrivateNet = false
	defer func() { kconf.Config.Basic.EnablePrivateNet = orig }()
	dialer := grpcSSRFContextDialer(2 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := dialer(ctx, "127.0.0.1:50051")
	require.Error(t, err)
	require.Contains(t, err.Error(), "in internal network")
}
