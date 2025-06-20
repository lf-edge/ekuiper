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
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

type RunDef struct {
	Id        string                    `json:"id"`
	Sql       string                    `json:"sql"`
	Mock      map[string]map[string]any `json:"mockSource"`
	SinkProps map[string]any            `json:"sinkProps"`

	endpoint string
}

func genTrialRuleID(def *RunDef) string {
	return "$$_" + uuid.New().String() + def.Id
}

func genTrialRule(rd *RunDef, sinkProps map[string]interface{}) *def.Rule {
	id := genTrialRuleID(rd)
	rt := def.GetDefaultRule(id, rd.Sql)
	rt.Actions = []map[string]interface{}{
		{
			"websocket": sinkProps,
		},
	}
	// Let trial rule always send out error to show
	rt.Options.SendError = true
	return rt
}

func create(def *RunDef) (*topo.Topo, error) {
	endpoint := "/test/" + def.Id
	def.endpoint = fmt.Sprintf("$$ws/%s", endpoint)
	sinkProps := map[string]any{
		"path":       endpoint,
		"sendError":  true,
		"datasource": endpoint,
	}
	cw, err := connection.FetchConnection(context.Background(), def.endpoint, "websocket", sinkProps, nil)
	if err != nil {
		return nil, err
	}
	_, err = cw.Wait(context.Background())
	if err != nil {
		return nil, err
	}

	for k, v := range def.SinkProps {
		sinkProps[k] = v
	}
	trialRule := genTrialRule(def, sinkProps)
	// Add trial run prefix for rule id to avoid duplicate rule id with real rules in runtime or other trial rule
	tp, _, err := planner.PlanSQLWithSourcesAndSinks(trialRule, def.Mock)
	if err != nil {
		return nil, fmt.Errorf("fail to run rule %s: %s", def.Id, err)
	}
	return tp, nil
}

func trialRun(tp *topo.Topo, endpoint string) {
	go func() {
		defer connection.DetachConnection(context.Background(), endpoint)
		timeout := time.After(5 * time.Minute)
		err := infra.SafeRun(func() error {
			select {
			case err := <-tp.Open():
				if errorx.IsUnexpectedErr(err) {
					conf.Log.Errorf("closing test run for error: %v", err)
					tp.Cancel()
					return err
				} else if errorx.IsEOF(err) {
					// If stop by EOF
					tp.Cancel()
					tp.GetContext().GetLogger().Debugf("trial run stops by EOF, wait for timeout")
					<-timeout
				} else {
					tp.Cancel()
					return nil
				}
			case <-timeout:
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
