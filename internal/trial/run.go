// Copyright 2023 EMQ Technologies Co., Ltd.
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

package trial

import (
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type RunDef struct {
	Id        string                    `json:"id"`
	Sql       string                    `json:"sql"`
	Mock      map[string]map[string]any `json:"mockSource"`
	SinkProps map[string]any            `json:"sinkProps"`
}

func create(def *RunDef) (*topo.Topo, api.MessageClient, error) {
	sinkProps := map[string]any{
		"path": "/test/" + def.Id,
	}
	for k, v := range def.SinkProps {
		sinkProps[k] = v
	}
	tp, err := planner.PlanSQLWithSourcesAndSinks(api.GetDefaultRule(def.Id, def.Sql), def.Mock, []*node.SinkNode{node.NewSinkNode("ws", "websocket", sinkProps)})
	if err != nil {
		return nil, nil, fmt.Errorf("fail to run rule %s: %s", def.Id, err)
	}
	// Create websocket client to send out control error message together with data
	cli, err := clients.GetClient("websocket", sinkProps)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to create websocket server for rule %s: %s", def.Id, err)
	}
	return tp, cli, nil
}

func trialRun(tp *topo.Topo, cli api.MessageClient) {
	go func() {
		timeout := time.NewTicker(5 * time.Minute)
		defer func() {
			timeout.Stop()
			contextLogger := conf.Log.WithField("trial run", 0)
			ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
			clients.ReleaseClient(ctx, cli)
		}()
		err := infra.SafeRun(func() error {
			select {
			case err := <-tp.Open():
				if err != nil {
					tp.GetContext().SetError(err)
					tp.Cancel()
					return err
				}
			case <-timeout.C:
				tp.GetContext().GetLogger().Debugf("trial run stops after timeout")
				tp.Cancel()
			}
			return nil
		})
		if err != nil {
			conf.Log.Errorf("closing test run for error: %v", err)
			_ = cli.Publish(tp.GetContext(), "", []byte(err.Error()), nil)
		}
	}()
}
