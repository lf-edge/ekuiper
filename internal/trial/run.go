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
	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
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
	return rt
}

func create(def *RunDef) (*topo.Topo, error) {
	endpoint := "/test/" + def.Id
	def.endpoint = endpoint
	_, _, err := httpserver.RegisterWebSocketEndpoint(context.Background(), endpoint)
	if err != nil {
		return nil, err
	}
	sinkProps := map[string]any{
		"path":      endpoint,
		"sendError": true,
	}
	for k, v := range def.SinkProps {
		sinkProps[k] = v
	}
	trialRule := genTrialRule(def, sinkProps)
	// Add trial run prefix for rule id to avoid duplicate rule id with real rules in runtime or other trial rule
	tp, err := planner.PlanSQLWithSourcesAndSinks(trialRule, prepareSourceProps(def.Mock))
	if err != nil {
		return nil, fmt.Errorf("fail to run rule %s: %s", def.Id, err)
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
		return nil, fmt.Errorf("fail to run rule %s: %s", def.Id, err)
	}
	tp.Cancel()
	return tp, nil
}

func trialRun(tp *topo.Topo) {
	go func() {
		timeout := time.After(5 * time.Minute)
		err := infra.SafeRun(func() error {
			select {
			case err := <-tp.Open():
				if err != nil {
					conf.Log.Errorf("closing test run for error: %v", err)
					tp.Cancel()
					return err
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

func prepareSourceProps(mockSource map[string]map[string]any) map[string]map[string]any {
	for source, props := range mockSource {
		v, ok := props["interval"]
		if ok {
			vi, ok := v.(int)
			if ok {
				props["interval"] = fmt.Sprintf("%dms", vi)
				mockSource[source] = props
				continue
			}
		}
	}
	return mockSource
}
