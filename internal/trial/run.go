// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

	"github.com/google/uuid"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/connection/clients"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

type RunDef struct {
	Id        string                    `json:"id"`
	Sql       string                    `json:"sql"`
	Mock      map[string]map[string]any `json:"mockSource"`
	SinkProps map[string]any            `json:"sinkProps"`
}

func create(runDef *RunDef) (*topo.Topo, io.MessageClient, error) {
	sinkProps := map[string]any{
		"path":      "/test/" + runDef.Id,
		"sendError": true,
	}
	for k, v := range runDef.SinkProps {
		sinkProps[k] = v
	}
	// TODO open this again
	// Add trial run prefix for rule id to avoid duplicate rule id with real rules in runtime or other trial rule
	//tp, err := planner.PlanSQLWithSourcesAndSinks(api.GetDefaultRule("$$_"+uuid.New().String()+runDef.Id, runDef.Sql), runDef.Mock, []node.DataSinkNode{node.NewSinkNode("ws", "websocket", sinkProps)})
	tp, err := planner.PlanSQLWithSourcesAndSinks(def.GetDefaultRule("$$_"+uuid.New().String()+runDef.Id, runDef.Sql), runDef.Mock)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to run rule %s: %s", runDef.Id, err)
	}
	// Try run
	// TODO currently some static validations are done in runtime, so start to run to detect them. This adds time penalty for this API.
	// 	In the future, we should do it in planning.
	err = infra.SafeRun(func() error {
		select {
		case e := <-tp.Open():
			if e != nil {
				return e
			}
		case <-time.After(10 * time.Millisecond):
			return nil
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("fail to run rule %s: %s", runDef.Id, err)
	}
	tp.Cancel()
	// Create websocket client to send out control error message together with data
	cli, err := clients.GetClient("websocket", sinkProps)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to create websocket server for rule %s: %s", runDef.Id, err)
	}
	return tp, cli, nil
}

func trialRun(tp *topo.Topo, cli io.MessageClient) {
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
					conf.Log.Errorf("closing test run for error: %v", err)
					_ = cli.Publish(tp.GetContext(), "", []byte(err.Error()), nil)
					// Wait for client connection
					time.Sleep(1 * time.Second)
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
			conf.Log.Debugf("trial run error: %v", err)
		}
	}()
}
