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

package neuron

import (
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/nng"
)

func ping(ctx api.StreamContext, props map[string]any) error {
	props["protocol"] = PROTOCOL
	cli := nng.CreateConnection(ctx)
	err := cli.Provision(ctx, props)
	if err != nil {
		return err
	}
	err = cli.Dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	time.Sleep(1000 * time.Millisecond)
	return cli.Ping(ctx)
}

func connect(ctx api.StreamContext, url string, props map[string]any, sc api.StatusChangeHandler) (modules.Connection, error) {
	ctx.GetLogger().Infof("Connecting to neuron")
	connId := PROTOCOL + url
	cw, err := connection.FetchConnection(ctx, connId, "nng", props, sc)
	if err != nil {
		return nil, err
	}
	return cw.Wait()
}

func close(ctx api.StreamContext, conn modules.Connection, url string, props map[string]any) {
	connId := PROTOCOL + url
	_ = connection.DetachConnection(ctx, connId, props)
	if conn != nil {
		conn.DetachSub(ctx, props)
	}
}
