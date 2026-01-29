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
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

// TrialManager Manager Initialized in the binder
var TrialManager = &Manager{
	runs: make(map[string]Run),
}

// Manager In memory manager for all trial rules
type Manager struct {
	syncx.RWMutex
	// ruleId -> *Topo
	runs map[string]Run
}

type Run struct {
	def  *RunDef
	topo *topo.Topo
}

func (m *Manager) CreateRule(ruleDef string) (string, error) {
	def := &RunDef{}
	err := json.Unmarshal([]byte(ruleDef), def)
	if err != nil {
		return "", fmt.Errorf("fail to parse rule definition %s: %s", ruleDef, err)
	}
	m.Lock()
	defer m.Unlock()
	// If the rule exists, stop it first
	if r, ok := m.runs[def.Id]; ok {
		r.topo.Cancel()
		conf.Log.Warnf("stop last run of test rule %s", def.Id)
	}
	t, err := create(def)
	if err != nil {
		return "", err
	}
	m.runs[def.Id] = Run{
		def:  def,
		topo: t,
	}
	return def.Id, nil
}

func (m *Manager) StopRule(ruleId string) {
	m.Lock()
	defer m.Unlock()
	if r, ok := m.runs[ruleId]; ok {
		r.topo.Cancel()
		delete(m.runs, ruleId)
		_ = connection.DetachConnection(context.Background(), r.def.endpoint)
	} else {
		conf.Log.Warnf("try to stop test rule %s but it is not found", ruleId)
	}
}

func (m *Manager) StartRule(ruleId string) error {
	m.RLock()
	defer m.RUnlock()
	if r, ok := m.runs[ruleId]; ok {
		trialRun(r.topo, r.def.endpoint)
	} else {
		return fmt.Errorf("try to start test rule %s but it is not found", ruleId)
	}
	return nil
}
