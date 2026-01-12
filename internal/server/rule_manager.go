// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/replace"
)

// Rule storage includes kv and in memory registry
// Kv stores the rule text with *expected* status so that the rule can be restored after restart
// Registry stores the current rule state in runtime
// Here registry is the in memory registry
var registry *RuleRegistry

type RuleRegistry struct {
	sync.RWMutex
	internal map[string]*rule.State
}

//// registry and db level state change functions

func (rr *RuleRegistry) update(key string, ruleJson string, value *rule.State) error {
	rr.Lock()
	defer rr.Unlock()
	rr.internal[key] = value
	return ruleProcessor.ExecUpsert(key, ruleJson)
}

// load the entry of a rule by id. It is used to get the current rule state
// or send command to a running rule
func (rr *RuleRegistry) load(key string) (value *rule.State, ok bool) {
	rr.RLock()
	result, ok := rr.internal[key]
	rr.RUnlock()
	return result, ok
}

func (rr *RuleRegistry) keys() (keys []string) {
	rr.RLock()
	defer rr.RUnlock()
	keys = make([]string, 0, len(rr.internal))
	for k := range rr.internal {
		keys = append(keys, k)
	}
	return
}

// save registers rule to in-memory registry and persists to DB atomically.
// It handles concurrent creation by checking registry under lock.
// It fails if the rule already exists.
func (rr *RuleRegistry) save(key string, ruleJson string, value *rule.State) error {
	rr.Lock()
	defer rr.Unlock()

	// Use ExecCreate (Setnx) logic via Upsert but we verified it doesn't exist
	if err := ruleProcessor.ExecCreate(key, ruleJson); err != nil {
		return err
	}
	// Update registry only after successful DB write
	rr.internal[key] = value
	return nil
}

// only register. It is called when recover from db
func (rr *RuleRegistry) register(key string, value *rule.State) {
	rr.Lock()
	defer rr.Unlock()
	rr.internal[key] = value
}

// upsert attempts to update the rule atomically.
// It checks version, persists to DB, and updates memory (either replacing the state or updating in-place) under lock.
// Returns: (liveState, wasUpdate, error)
//   - liveState: the state that should be used for lifecycle management
//   - wasUpdate: true if an existing rule was updated in-place, false if a new rule was inserted
func (rr *RuleRegistry) upsert(id string, ruleJson string, newRuleDef *def.Rule, newState *rule.State) (*rule.State, bool, error) {
	rr.Lock()
	defer rr.Unlock()

	// 1. Check for existing rule and validate version
	existing, exists := rr.internal[id]
	if exists {
		if !ruleProcessor.CanReplace(existing.Rule.Version, newRuleDef.Version) {
			return nil, false, fmt.Errorf("rule %s already exists with version (%s), new version (%s) is lower", id, existing.Rule.Version, newRuleDef.Version)
		}
	}

	// 2. Persist to DB
	if !newRuleDef.Temp {
		if err := ruleProcessor.ExecUpsert(id, ruleJson); err != nil {
			return nil, false, err
		}
	}

	// 3. Update Memory
	if exists {
		// In-place update: update the rule definition pointer while holding the lock
		existing.Rule = newRuleDef
		return existing, true, nil
	} else if newState != nil {
		// New rule: insert the candidate state
		rr.internal[id] = newState
		return newState, false, nil
	}

	return nil, false, fmt.Errorf("unexpected state: rule %s not found and no new state provided", id)
}

func (rr *RuleRegistry) updateTrigger(id string, trigger bool) error {
	rr.Lock()
	defer rr.Unlock()
	err := ruleProcessor.ExecReplaceRuleState(id, trigger)
	return err
}

func (rr *RuleRegistry) delete(key string) (*rule.State, error) {
	rr.Lock()
	defer rr.Unlock()
	var err error
	result, ok := rr.internal[key]
	if ok {
		delete(rr.internal, key)
		err = ruleProcessor.ExecDrop(key)
	} else {
		err = fmt.Errorf("rule %s not found", key)
	}
	return result, err
}

//// APIs for REST service

func (rr *RuleRegistry) CreateRule(name, ruleJson string) (id string, err error) {
	// Validate the rule json
	r, err := ruleProcessor.GetRuleByJson(name, ruleJson)
	if err != nil {
		return "", fmt.Errorf("invalid rule json: %v", err)
	}
	if _, ok := rr.load(r.Id); ok {
		return name, fmt.Errorf("rule %s already exists", r.Id)
	}
	ruleJson = replace.ReplaceRuleJson(ruleJson, conf.IsTesting)
	// create state and save
	rs := rule.NewState(r, func(id string, b bool) {
		err = rr.updateTrigger(id, b)
		if err != nil {
			conf.Log.Warnf("update trigger error: %v", err)
		}
	})
	// Validate the topo
	tp, err := rs.Validate()
	if err != nil {
		return r.Id, err
	}
	// Store to registry and KV
	err = rr.save(r.Id, ruleJson, rs)
	if err != nil {
		return r.Id, fmt.Errorf("store the rule error: %v", err)
	}
	// Start the rule asyncly
	if r.Triggered {
		rs.WithTopo(tp)
		go func() {
			panicOrError := infra.SafeRun(func() error {
				// Start the rule which runs async
				return rs.Start()
			})
			if panicOrError != nil {
				logger.Errorf("Rule %s start failed: %s", r.Id, panicOrError)
			}
		}()
	} else if tp != nil {
		_ = tp.Cancel()
	}
	return r.Id, nil
}

// RecoverRule loads in imported rule.
// Unlike creation, 1. it supposes the rule is valid thus, it will always create the rule state in registry
// 2. It does not handle rule saving to db.
func (rr *RuleRegistry) RecoverRule(r *def.Rule) string {
	rs := rule.NewState(r, func(id string, b bool) {
		err := rr.updateTrigger(id, b)
		if err != nil {
			conf.Log.Warnf("update trigger error: %v", err)
		}
	})
	rr.register(r.Id, rs)
	if !r.Triggered {
		return fmt.Sprintf("Rule %s was stopped.", r.Id)
	} else {
		panicOrError := infra.SafeRun(func() error {
			// Start the rule which runs async
			return rs.Start()
		})
		if panicOrError != nil {
			return fmt.Sprintf("Rule %s start failed: %s", r.Id, panicOrError)
		}
	}
	return fmt.Sprintf("Rule %s was started.", r.Id)
}

// UpsertRule validates the new rule, then update the db, then restart the rule
func (rr *RuleRegistry) UpsertRule(ruleId, ruleJson string) error {
	ruleJson = replace.ReplaceRuleJson(ruleJson, conf.IsTesting)
	// 1. Validate the rule json
	r, err := ruleProcessor.GetRuleByJson(ruleId, ruleJson)
	if err != nil {
		return fmt.Errorf("Invalid rule json: %v", err)
	}

	// 2. Create Candidate State for validation and potential insertion.
	// Note: For updates, this state is used only for validation (planning the topology).
	// The upsert() will return the existing state for updates, so this becomes temporary.
	// This is intentional - we need a State object to call Validate().
	candidateRS := rule.NewState(r, func(id string, b bool) {
		err = rr.updateTrigger(id, b)
		if err != nil {
			conf.Log.Warnf("update trigger error: %v", err)
		}
	})

	// 3. Validation - checks planning and static analysis
	newTopo, err := candidateRS.Validate()
	if err != nil {
		return err
	}

	// 4. Atomic Upsert
	// If exists: update in-place, return existing state (candidateRS becomes garbage)
	// If new: insert candidateRS, return candidateRS
	liveRS, wasUpdate, err := rr.upsert(r.Id, ruleJson, r, candidateRS)
	if err != nil {
		return err
	}

	// 5. Lifecycle Management on the LIVE state
	if wasUpdate {
		// Stop old instance
		liveRS.Stop()
	}

	liveRS.WithTopo(newTopo)
	if r.Triggered {
		err2 := liveRS.Start()
		if err2 != nil {
			return err2
		}
	} else if newTopo != nil {
		_ = newTopo.Cancel()
		liveRS.WithTopo(nil)
	}
	return nil
}

func (rr *RuleRegistry) DeleteRule(name string) error {
	// lock registry and db. rs level has its own lock
	rs, err := rr.delete(name)
	if rs != nil {
		err = rs.Delete()
		if err != nil {
			logger.Errorf("delete rule %s error: %v", name, err)
		}
		deleteRuleMetrics(name)
	}
	deleteRuleData(name)
	return err
}

func (rr *RuleRegistry) StartRule(name string) error {
	rs, ok := registry.load(name)
	if !ok {
		return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found in registry, please check if it is created", name))
	} else {
		err := rr.updateTrigger(name, true)
		if err != nil {
			conf.Log.Warnf("start rule update db status error: %s", err.Error())
		}
		return rs.Start()
	}
}

func (rr *RuleRegistry) StopRule(name string) error {
	if rs, ok := registry.load(name); ok {
		err := rr.updateTrigger(name, false)
		if err != nil {
			conf.Log.Warnf("stop rule update db status error: %s", err.Error())
		}
		rs.Stop()
	} else {
		return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found in registry, please check if it is created", name))
	}
	return nil
}

func (rr *RuleRegistry) RestartRule(name string) error {
	if rs, ok := registry.load(name); ok {
		err := rr.updateTrigger(name, true)
		if err != nil {
			conf.Log.Warnf("restart rule update db status error: %s", err.Error())
		}
		rs.Stop()
		rs.Rule, err = ruleProcessor.GetRuleById(name)
		if err != nil {
			return err
		}
		return rs.Start()
	} else {
		return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found in registry, please check if it is created", name))
	}
}

func (rr *RuleRegistry) GetAllRuleStatus() (string, error) {
	rules, err := ruleProcessor.GetAllRules()
	if err != nil {
		return "", err
	}
	keys := rr.keys()
	all := mergeAndSortStrings(rules, keys)
	m := make(map[string]ruleExceptionStatus)
	for _, ruleID := range all {
		s, err := getRuleExceptionStatus(ruleID)
		if err != nil {
			return "", err
		}
		m[ruleID] = s
	}
	b, _ := json.Marshal(m)
	return string(b), nil
}

func (rr *RuleRegistry) GetAllRulesWithStatus() ([]map[string]any, error) {
	ruleIds, err := ruleProcessor.GetAllRules()
	if err != nil {
		return nil, err
	}
	keys := rr.keys()
	all := mergeAndSortStrings(ruleIds, keys)
	result := make([]map[string]any, len(all))
	for i, id := range all {
		ruleName := id
		ruleDef, _ := ruleProcessor.GetRuleById(id)
		var tags []string
		if ruleDef != nil {
			if ruleDef.Name != "" {
				ruleName = ruleDef.Name
			}
			tags = ruleDef.Tags
		}
		var str string
		s, err := getRuleState(id)
		if err != nil {
			str = fmt.Sprintf("error: %s", err)
		} else {
			str = rule.StateName[s]
		}
		trace := false
		if str == "running" {
			rs, ok := registry.load(id)
			if ok {
				trace = rs.IsTraceEnabled()
			}
		}
		ver := ""
		if ruleDef != nil {
			ver = ruleDef.Version
		}
		result[i] = map[string]any{
			"id":      id,
			"name":    ruleName,
			"status":  str,
			"version": ver,
			"trace":   trace,
			"tags":    tags,
		}
	}
	return result, nil
}

// mergeAndSortStrings merges two string slices, removes duplicates, and sorts the result.
func mergeAndSortStrings(slice1, slice2 []string) []string {
	// Step 1: Merge the two slices
	merged := make([]string, 0, len(slice1)+len(slice2)) // Pre-allocate capacity
	merged = append(merged, slice1...)
	merged = append(merged, slice2...)

	// Step 2: Remove duplicates using a map
	seen := make(map[string]struct{})        // Using struct{} for a memory-efficient "set"
	unique := make([]string, 0, len(merged)) // Pre-allocate capacity based on merged length (upper bound)

	for _, s := range merged {
		if _, ok := seen[s]; !ok { // If element not seen yet
			seen[s] = struct{}{}       // Mark as seen
			unique = append(unique, s) // Add to unique slice
		}
	}

	// Step 3: Sort the unique slice
	sort.Strings(unique) // sort.Strings sorts a slice of strings in ascending order

	return unique
}

func (rr *RuleRegistry) GetRuleStatus(name string) (string, error) {
	if rs, ok := registry.load(name); ok {
		return rs.GetStatusMessage(), nil
	} else {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}

func (rr *RuleRegistry) GetRuleStatusV2(name string) (map[string]any, error) {
	if rs, ok := rr.load(name); ok {
		return rs.GetStatusMap(), nil
	} else {
		return nil, errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}

func (rr *RuleRegistry) GetRuleTopo(name string) (string, error) {
	if rs, ok := registry.load(name); ok {
		graph := rs.GetTopoGraph()
		if graph == nil {
			return "", errorx.New(fmt.Sprintf("Fail to get rule %s's topo, make sure the rule has been started before", name))
		}
		bs, err := json.Marshal(graph)
		if err != nil {
			return "", errorx.New(fmt.Sprintf("Fail to encode rule %s's topo", name))
		} else {
			return string(bs), nil
		}
	} else {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}

func (rr *RuleRegistry) GetRuleSinkSchema(name string) (map[string]*ast.JsonStreamField, error) {
	if rs, ok := registry.load(name); ok {
		if !rs.HasTopo() {
			return nil, errorx.New(fmt.Sprintf("Fail to get rule %s's topo, make sure the rule has been started before", name))
		}
		return rs.GetSchema(), nil
	} else {
		return nil, errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}

func (rr *RuleRegistry) ValidateRule(name, ruleJson string) ([]string, bool, error) {
	// Validate the ruleDef json
	ruleDef, err := ruleProcessor.GetRuleByJson(name, ruleJson)
	if err != nil {
		return nil, false, fmt.Errorf("invalid rule json: %v", err)
	}
	var sources []string
	if len(ruleDef.Sql) > 0 {
		stmt, _ := xsql.GetStatementFromSql(ruleDef.Sql)
		s, err := store.GetKV("stream")
		if err != nil {
			return nil, false, err
		}
		sources = xsql.GetStreams(stmt)
		for _, result := range sources {
			_, err := xsql.GetDataSource(s, result)
			if err != nil {
				return nil, false, err
			}
		}
	} else if ruleDef.Graph != nil {
		tp, err := planner.PlanByGraph(ruleDef)
		if err != nil {
			return nil, false, fmt.Errorf("invalid ruleDef graph: %v", err)
		}
		sources = tp.GetTopo().Sources
	}
	return sources, true, nil
}

/// Rule Scheduler internal API

func (rr *RuleRegistry) scheduledStart(name string) error {
	rs, ok := registry.load(name)
	if !ok {
		return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Scheduled rule %s is not found in registry, please check if it is deleted", name))
	} else {
		return rs.ScheduleStart()
	}
}

func (rr *RuleRegistry) scheduledStop(name string) error {
	rs, ok := registry.load(name)
	if !ok {
		return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Scheduled rule %s is not found in registry, please check if it is deleted", name))
	} else {
		rs.ScheduleStop()
		return nil
	}
}

func (rr *RuleRegistry) stopAtExit(name string, msg string) error {
	rs, ok := registry.load(name)
	if !ok {
		return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found in registry, please check if it is deleted", name))
	} else {
		if len(msg) == 0 {
			msg = "canceled manually"
		}
		rs.StopWithLastWillAndSig(msg, xsql.SigTerm)
		return nil
	}
}

//// Util functions

func getRuleExceptionStatus(name string) (ruleExceptionStatus, error) {
	s := ruleExceptionStatus{
		lastExceptionTime: -1,
	}
	if rs, ok := registry.load(name); ok {
		st := rs.GetState()
		s.Status = rule.StateName[st]
		if st == rule.Running {
			keys, values := rs.GetMetrics()
			for i, key := range keys {
				if strings.Contains(key, "last_exception_time") {
					v := values[i].(int64)
					if v > s.lastExceptionTime {
						s.lastExceptionTime = v
						total, last := getTargetException(keys, values, key[:strings.Index(key, "_last_exception_time")])
						s.LastException = last
						s.ExceptionsTotal = total
					}
				}
			}
		}
	}
	return s, nil
}

func getTargetException(keys []string, values []any, prefix string) (int64, string) {
	var t int64
	lastException := ""
	for i, key := range keys {
		if key == fmt.Sprintf("%s_exceptions_total", prefix) {
			t = values[i].(int64)
			continue
		}
		if key == fmt.Sprintf("%s_last_exception", prefix) {
			lastException = values[i].(string)
			continue
		}
	}
	return t, lastException
}

type ruleExceptionStatus struct {
	Status            string `json:"status"`
	LastException     string `json:"last_exception"`
	ExceptionsTotal   int64  `json:"exceptions_total"`
	lastExceptionTime int64
}

type ruleWrapper struct {
	rule      *def.Rule
	state     rule.RunState
	startTime time.Time
}

func getAllRulesWithState() ([]ruleWrapper, error) {
	ruleIds, err := ruleProcessor.GetAllRules()
	if err != nil {
		return nil, err
	}
	sort.Strings(ruleIds)
	rules := make([]ruleWrapper, 0, len(ruleIds))
	for _, id := range ruleIds {
		rs, ok := registry.load(id)
		if ok {
			s := rs.GetState()
			rules = append(rules, ruleWrapper{rule: rs.Rule, state: s, startTime: rs.GetStartTimestamp()})
		}
	}
	return rules, nil
}

func getRuleState(name string) (rule.RunState, error) {
	if rs, ok := registry.load(name); ok {
		return rs.GetState(), nil
	} else {
		return rule.Stopped, fmt.Errorf("Rule %s is not found in registry", name)
	}
}

func deleteRuleMetrics(name string) {
	if conf.Config != nil && conf.Config.Basic.Prometheus {
		metrics.RemoveRuleStatus(name)
	}
}

func deleteRuleData(name string) {
	dataLoc, err := conf.GetDataLoc()
	if err != nil {
		conf.Log.Errorf("delete rule data error: %v", err)
		return
	}
	ruleDataPath := filepath.Join(dataLoc, "rule_"+name)
	err = os.RemoveAll(ruleDataPath)
	if err != nil {
		conf.Log.Errorf("delete rule data error: %v", err)
	} else {
		conf.Log.Infof("delete rule data: %s", ruleDataPath)
	}
}
