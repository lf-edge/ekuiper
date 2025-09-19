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

package processor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
	"github.com/lf-edge/ekuiper/v2/pkg/validate"
)

type RuleProcessor struct {
	db           kv.KeyValue
	ruleStatusDb kv.KeyValue
}

func NewRuleProcessor() *RuleProcessor {
	db, err := store.GetKV("rule")
	if err != nil {
		panic(fmt.Sprintf("Can not initialize store for the rule processor at path 'rule': %v", err))
	}
	ruleStatusDb, err := store.GetKV("ruleStatus")
	if err != nil {
		panic(fmt.Sprintf("Can not initialize store for the rule processor at path 'rule': %v", err))
	}
	processor := &RuleProcessor{
		db:           db,
		ruleStatusDb: ruleStatusDb,
	}
	return processor
}

func (p *RuleProcessor) ExecCreateWithValidation(name, ruleJson string) (*def.Rule, error) {
	rule, err := p.GetRuleByJson(name, ruleJson)
	if err != nil {
		return nil, err
	}

	or, err := p.GetRuleById(rule.Id)
	if err == nil {
		if !CanReplace(or.Version, rule.Version) { // old rule has newer version
			return nil, fmt.Errorf("rule %s already exists with version (%s), new version (%s) is lower", rule.Id, or.Version, rule.Version)
		}
	}

	if !rule.Temp {
		err = p.db.Set(rule.Id, ruleJson)
		if err != nil {
			return nil, err
		}
	}
	log.Infof("Rule %s with version (%s) is created.", rule.Id, rule.Version)
	return rule, nil
}

func (p *RuleProcessor) ExecCreate(name, ruleJson string) error {
	rule, err := p.GetRuleByJson(name, ruleJson)
	if err != nil {
		return err
	}
	if !rule.Temp {
		err := p.db.Setnx(name, ruleJson)
		if err != nil {
			return err
		}
	}
	log.Infof("Rule %s is created.", name)
	return nil
}

func (p *RuleProcessor) ExecUpsert(id, ruleJson string) error {
	rule, err := p.GetRuleByJson(id, ruleJson)
	if err != nil {
		return err
	}
	if !rule.Temp {
		err = p.db.Set(id, ruleJson)
		if err != nil {
			return err
		}
	} else {
		_ = p.db.Delete(id)
	}
	log.Infof("Rule %s is upserted.", id)
	return nil
}

func (p *RuleProcessor) ExecReplaceRuleState(name string, triggered bool) error {
	ruleStr, err := p.GetRuleJson(name)
	if err != nil {
		return err
	}

	ruleMap := map[string]interface{}{}
	err = json.Unmarshal([]byte(ruleStr), &ruleMap)
	if err != nil {
		return fmt.Errorf("Unmarshal rule %s error : %s.", name, err)
	}
	ruleMap["triggered"] = triggered
	ruleJson, err := json.Marshal(ruleMap)
	if err != nil {
		return fmt.Errorf("Marshal rule %s error : %s.", name, err)
	}

	isTemp := false
	if tempBool, ok := ruleMap["temp"].(bool); ok {
		isTemp = tempBool
	}

	if !isTemp {
		err = p.db.Set(name, string(ruleJson))
		if err != nil {
			return err
		}
	}
	log.Infof("Rule %s is replaced.", name)
	return err
}

func (p *RuleProcessor) GetRuleJson(id string) (string, error) {
	var s1 string
	f, _ := p.db.Get(id, &s1)
	if !f {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found.", id))
	}
	return s1, nil
}

func (p *RuleProcessor) GetRuleById(id string) (*def.Rule, error) {
	var s1 string
	f, _ := p.db.Get(id, &s1)
	if !f {
		return nil, errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found.", id))
	}
	return p.GetRuleByJsonValidated(id, s1)
}

// GetRuleByJsonValidated called when the json is getting from trusted source like db
func (p *RuleProcessor) GetRuleByJsonValidated(id, ruleJson string) (*def.Rule, error) {
	opt := conf.Config.Rule
	// set default rule options
	rule := &def.Rule{
		Triggered: true,
		Options:   clone(opt),
		Id:        id,
	}
	if err := json.Unmarshal(cast.StringToBytes(ruleJson), &rule); err != nil {
		return nil, fmt.Errorf("Parse rule %s error : %s.", ruleJson, err)
	}
	if rule.Options == nil {
		rule.Options = &opt
	}
	return rule, nil
}

func (p *RuleProcessor) GetRuleByJson(id, ruleJson string) (*def.Rule, error) {
	rule, err := p.GetRuleByJsonValidated(id, ruleJson)
	if err != nil {
		return rule, err
	}
	// validation
	if rule.Id == "" {
		return nil, fmt.Errorf("Missing rule id.")
	}
	if id != "" && rule.Id != "" && id != rule.Id {
		return nil, fmt.Errorf("RuleId is not consistent with rule id.")
	}
	if err := validateRuleID(rule.Id); err != nil {
		return nil, err
	}
	if rule.Sql != "" {
		if rule.Graph != nil {
			return nil, fmt.Errorf("Rule %s has both sql and graph.", rule.Id)
		}
		if _, err := xsql.GetStatementFromSql(rule.Sql); err != nil {
			return nil, err
		}
		if rule.Actions == nil || len(rule.Actions) == 0 {
			return nil, fmt.Errorf("Missing rule actions.")
		}
	} else {
		if rule.Graph == nil {
			return nil, fmt.Errorf("Rule %s has neither sql nor graph.", rule.Id)
		}
	}
	err = conf.ValidateRuleOption(rule.Options)
	if err != nil {
		return nil, fmt.Errorf("Rule %s has invalid options: %s.", rule.Id, err)
	}
	return rule, nil
}

// CanReplace compare which version is newer, return true if new version is newer
// If both version are empty, need to replace to be backward compatible
func CanReplace(old, new string) bool {
	if old == "" && new == "" {
		return true
	}
	return new > old
}

func validateRuleID(id string) error {
	return validate.ValidateID(id)
}

func clone(opt def.RuleOption) *def.RuleOption {
	return &def.RuleOption{
		IsEventTime:        opt.IsEventTime,
		LateTol:            opt.LateTol,
		Concurrency:        opt.Concurrency,
		BufferLength:       opt.BufferLength,
		SendMetaToSink:     opt.SendMetaToSink,
		SendError:          opt.SendError,
		Qos:                opt.Qos,
		CheckpointInterval: opt.CheckpointInterval,
		RestartStrategy: &def.RestartStrategy{
			Attempts:     opt.RestartStrategy.Attempts,
			Delay:        opt.RestartStrategy.Delay,
			Multiplier:   opt.RestartStrategy.Multiplier,
			MaxDelay:     opt.RestartStrategy.MaxDelay,
			JitterFactor: opt.RestartStrategy.JitterFactor,
		},
	}
}

func (p *RuleProcessor) ExecExists(name string) bool {
	var s1 string
	f, _ := p.db.Get(name, &s1)
	return f
}

func (p *RuleProcessor) ExecDesc(name string) (string, error) {
	var s1 string
	f, _ := p.db.Get(name, &s1)
	if !f {
		return "", fmt.Errorf("Rule %s is not found.", name)
	}
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, cast.StringToBytes(s1), "", "  "); err != nil {
		return "", err
	}

	return fmt.Sprintln(dst.String()), nil
}

func (p *RuleProcessor) GetAllRules() ([]string, error) {
	return p.db.Keys()
}

func (p *RuleProcessor) GetAllRulesJson() (map[string]string, error) {
	return p.db.All()
}

func (p *RuleProcessor) ExecDrop(name string) error {
	var (
		ruleJson string
		allErr   error
	)
	if ok, _ := p.db.Get(name, &ruleJson); ok {
		if err := cleanSinkCache(name); err != nil {
			allErr = errors.Join(allErr, fmt.Errorf("Clean sink cache failed: %v.", err))
		}
		if err := cleanCheckpoint(name); err != nil {
			allErr = errors.Join(allErr, fmt.Errorf("Clean checkpoint cache failed: %v.", err))
		}

	}
	err := p.db.Delete(name)
	if err != nil {
		allErr = errors.Join(allErr, fmt.Errorf("Delete rule %s failed: %v.", name, err))
	}
	return allErr
}

func cleanCheckpoint(name string) error {
	err := store.DropTS(name)
	if err != nil {
		return err
	}
	return nil
}

func cleanSinkCache(name string) error {
	err := store.DropCacheKVForRule(name)
	if err != nil {
		return err
	}
	return nil
}
