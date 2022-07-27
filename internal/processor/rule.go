// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/kv"
)

type RuleProcessor struct {
	db kv.KeyValue
}

func NewRuleProcessor() *RuleProcessor {
	err, db := store.GetKV("rule")
	if err != nil {
		panic(fmt.Sprintf("Can not initalize store for the rule processor at path 'rule': %v", err))
	}
	processor := &RuleProcessor{
		db: db,
	}
	return processor
}

func (p *RuleProcessor) ExecCreate(name, ruleJson string) (*api.Rule, error) {
	rule, err := p.getRuleByJson(name, ruleJson)
	if err != nil {
		return nil, err
	}

	err = p.db.Setnx(rule.Id, ruleJson)
	if err != nil {
		return nil, err
	} else {
		log.Infof("Rule %s is created.", rule.Id)
	}

	return rule, nil
}
func (p *RuleProcessor) ExecUpdate(name, ruleJson string) (*api.Rule, error) {
	rule, err := p.getRuleByJson(name, ruleJson)
	if err != nil {
		return nil, err
	}

	err = p.db.Set(rule.Id, ruleJson)
	if err != nil {
		return nil, err
	} else {
		log.Infof("Rule %s is update.", rule.Id)
	}

	return rule, nil
}

func (p *RuleProcessor) ExecReplaceRuleState(name string, triggered bool) (err error) {
	rule, err := p.GetRuleById(name)
	if err != nil {
		return err
	}

	rule.Triggered = triggered
	ruleJson, err := json.Marshal(rule)
	if err != nil {
		return fmt.Errorf("Marshal rule %s error : %s.", name, err)
	}

	err = p.db.Set(name, string(ruleJson))
	if err != nil {
		return err
	} else {
		log.Infof("Rule %s is replaced.", name)
	}
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

func (p *RuleProcessor) GetRuleById(id string) (*api.Rule, error) {
	var s1 string
	f, _ := p.db.Get(id, &s1)
	if !f {
		return nil, errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found.", id))
	}
	return p.getRuleByJson(id, s1)
}

func (p *RuleProcessor) getDefaultRule(name, sql string) *api.Rule {
	return &api.Rule{
		Id:  name,
		Sql: sql,
		Options: &api.RuleOption{
			IsEventTime:        false,
			LateTol:            1000,
			Concurrency:        1,
			BufferLength:       1024,
			SendMetaToSink:     false,
			SendError:          true,
			Qos:                api.AtMostOnce,
			CheckpointInterval: 300000,
		},
	}
}

func (p *RuleProcessor) getRuleByJson(id, ruleJson string) (*api.Rule, error) {
	opt := conf.Config.Rule
	//set default rule options
	rule := &api.Rule{
		Options: &opt,
	}
	if err := json.Unmarshal([]byte(ruleJson), &rule); err != nil {
		return nil, fmt.Errorf("Parse rule %s error : %s.", ruleJson, err)
	}

	//validation
	if rule.Id == "" && id == "" {
		return nil, fmt.Errorf("Missing rule id.")
	}
	if id != "" && rule.Id != "" && id != rule.Id {
		return nil, fmt.Errorf("RuleId is not consistent with rule id.")
	}
	if rule.Id == "" {
		rule.Id = id
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
	if rule.Options == nil {
		rule.Options = &opt
	}
	//Setnx default options
	if rule.Options.CheckpointInterval < 0 {
		return nil, fmt.Errorf("rule option checkpointInterval %d is invalid, require a positive integer", rule.Options.CheckpointInterval)
	}
	if rule.Options.Concurrency < 0 {
		return nil, fmt.Errorf("rule option concurrency %d is invalid, require a positive integer", rule.Options.Concurrency)
	}
	if rule.Options.BufferLength < 0 {
		return nil, fmt.Errorf("rule option bufferLength %d is invalid, require a positive integer", rule.Options.BufferLength)
	}
	if rule.Options.LateTol < 0 {
		return nil, fmt.Errorf("rule option lateTolerance %d is invalid, require a positive integer", rule.Options.LateTol)
	}
	return rule, nil
}

func (p *RuleProcessor) ExecDesc(name string) (string, error) {
	var s1 string
	f, _ := p.db.Get(name, &s1)
	if !f {
		return "", fmt.Errorf("Rule %s is not found.", name)
	}
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, []byte(s1), "", "  "); err != nil {
		return "", err
	}

	return fmt.Sprintln(dst.String()), nil
}

func (p *RuleProcessor) GetAllRules() ([]string, error) {
	return p.db.Keys()
}

func (p *RuleProcessor) ExecDrop(name string) (string, error) {
	result := fmt.Sprintf("Rule %s is dropped.", name)
	var ruleJson string
	if ok, _ := p.db.Get(name, &ruleJson); ok {
		if err := cleanSinkCache(name); err != nil {
			result = fmt.Sprintf("%s. Clean sink cache faile: %s.", result, err)
		}
		if err := cleanCheckpoint(name); err != nil {
			result = fmt.Sprintf("%s. Clean checkpoint cache faile: %s.", result, err)
		}

	}
	err := p.db.Delete(name)
	if err != nil {
		return "", err
	} else {
		return result, nil
	}
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
