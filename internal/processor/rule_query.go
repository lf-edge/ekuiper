// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package processor

import (
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

func (p *RuleProcessor) ExecQuery(ruleid, sql string) (*topo.Topo, error) {
	if tp, _, err := planner.PlanSQLWithSourcesAndSinks(def.GetDefaultRule(ruleid, sql), nil); err != nil {
		return nil, err
	} else {
		go func() {
			err := infra.SafeRun(func() error {
				select {
				case err := <-tp.Open():
					if errorx.IsUnexpectedErr(err) {
						tp.GetContext().SetError(err)
						tp.Cancel()
						return err
					}
				}
				return nil
			})
			if err != nil {
				log.Infof("closing query for error: %v", err)
			} else {
				log.Info("closing query")
			}
		}()
		return tp, nil
	}
}
