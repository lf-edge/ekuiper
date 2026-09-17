// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"io"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type RulesetProcessor struct {
	r              *RuleProcessor
	s              *StreamProcessor
	newInitBackoff func() backoff.BackOff
}

type Ruleset struct {
	Streams map[string]string `json:"streams"`
	Tables  map[string]string `json:"tables"`
	Rules   map[string]string `json:"rules"`
}

func NewRulesetProcessor(r *RuleProcessor, s *StreamProcessor) *RulesetProcessor {
	return &RulesetProcessor{
		r: r,
		s: s,
		newInitBackoff: func() backoff.BackOff {
			return backoff.WithMaxRetries(backoff.NewConstantBackOff(100*time.Millisecond), 2)
		},
	}
}

func (rs *RulesetProcessor) Export() (io.ReadSeeker, []int, error) {
	var all Ruleset
	allStreams, err := rs.s.GetAll()
	if err != nil {
		return nil, nil, fmt.Errorf("fail to get all streams: %v", err)
	}
	all.Streams = allStreams["streams"]
	all.Tables = allStreams["tables"]
	rules, err := rs.r.GetAllRulesJson()
	if err != nil {
		return nil, nil, fmt.Errorf("fail to get all rules: %v", err)
	}
	all.Rules = rules
	jsonBytes, err := json.Marshal(all)
	if err != nil {
		return nil, nil, err
	}
	counts := []int{len(all.Streams), len(all.Tables), len(all.Rules)}
	return bytes.NewReader(jsonBytes), counts, nil
}

func (rs *RulesetProcessor) ExportRuleSet() *Ruleset {
	all := &Ruleset{}
	allStreams, err := rs.s.GetAll()
	if err != nil {
		conf.Log.Errorf("fail to get all streams: %v", err)
		return nil
	}
	all.Streams = allStreams["streams"]
	all.Tables = allStreams["tables"]
	rules, err := rs.r.GetAllRulesJson()
	if err != nil {
		conf.Log.Errorf("fail to get all rules: %v", err)
		return nil
	}
	all.Rules = rules
	return all
}

func (rs *RulesetProcessor) ExportRuleSetStatus() *Ruleset {
	all := &Ruleset{}
	allStreams, err := rs.s.streamStatusDb.All()
	if err != nil {
		conf.Log.Errorf("fail to get all stream status: %v", err)
		return nil
	}
	allTables, err := rs.s.tableStatusDb.All()
	if err != nil {
		conf.Log.Errorf("fail to get all table status: %v", err)
		return nil
	}
	all.Streams = allStreams
	all.Tables = allTables
	rules, err := rs.r.ruleStatusDb.All()
	if err != nil {
		conf.Log.Errorf("fail to get all rule status: %v", err)
		return nil
	}
	all.Rules = rules
	return all
}

func (rs *RulesetProcessor) Import(content []byte) ([]string, []int, error) {
	all := &Ruleset{}
	err := json.Unmarshal(content, all)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid import file: %v", err)
	}
	counts := make([]int, 3)
	// restore streams
	for k, v := range all.Streams {
		_, e := rs.s.ExecReplaceStream(k, v, ast.TypeStream)
		if e != nil {
			conf.Log.Warnf("Fail to import stream %s with error: %v", k, e)
		} else {
			counts[0]++
		}
	}
	// restore tables
	for k, v := range all.Tables {
		_, e := rs.s.ExecReplaceStream(k, v, ast.TypeTable)
		if e != nil {
			conf.Log.Warnf("Fail to import table %s with error: %v", k, e)
		} else {
			counts[1]++
		}
	}
	var rules []string
	// restore rules
	for k, v := range all.Rules {
		_, e := rs.r.ExecCreateWithValidation(k, v)
		if e != nil {
			conf.Log.Warnf("Fail to import rule %s with error: %v", k, e)
		} else {
			rules = append(rules, k)
			counts[2]++
		}
	}
	return rules, counts, nil
}

type InitOutcome int

const (
	InitApplied InitOutcome = iota
	InitSkipped
	InitTerminalError
	InitRetryableError
)

type InitObjectResult struct {
	Kind     string
	Name     string
	Outcome  InitOutcome
	Err      error
	Attempts int
}

type InitCounts struct {
	Streams int
	Tables  int
	Rules   int
}

type InitImportResult struct {
	Counts  InitCounts
	Objects []InitObjectResult
}

func (r InitImportResult) HasRetryableFailure() bool {
	for _, object := range r.Objects {
		if object.Outcome == InitRetryableError {
			return true
		}
	}
	return false
}

// ImportForInit applies the same object creation semantics as Import, but
// preserves per-object errors for the startup initializer. The REST/RPC import
// endpoints continue to use Import without automatic retries.
func (rs *RulesetProcessor) ImportForInit(content []byte) (InitImportResult, error) {
	var all Ruleset
	if err := json.Unmarshal(content, &all); err != nil {
		return InitImportResult{}, fmt.Errorf("invalid import file: %v", err)
	}
	var result InitImportResult
	for name, statement := range all.Streams {
		result.add(rs.applySource(name, statement, ast.TypeStream))
	}
	for name, statement := range all.Tables {
		result.add(rs.applySource(name, statement, ast.TypeTable))
	}
	for name, ruleJSON := range all.Rules {
		result.add(rs.applyRule(name, ruleJSON))
	}
	return result, nil
}

func (r *InitImportResult) add(object InitObjectResult) {
	r.Objects = append(r.Objects, object)
	if object.Outcome != InitApplied {
		return
	}
	switch object.Kind {
	case "stream":
		r.Counts.Streams++
	case "table":
		r.Counts.Tables++
	case "rule":
		r.Counts.Rules++
	}
}

func (rs *RulesetProcessor) applySource(name, statement string, kind ast.StreamType) InitObjectResult {
	r := InitObjectResult{Kind: ast.StreamTypeMap[kind], Name: name, Outcome: InitTerminalError, Attempts: 1}
	source, err := parseReplaceSource(name, statement, kind)
	if err != nil {
		r.Err = err
		return r
	}
	var stored string
	exists, err := rs.s.db.Get(name, &stored)
	if err != nil {
		r.Outcome, r.Err = InitRetryableError, err
		return r
	}
	if exists {
		var info xsql.StreamInfo
		if err := json.Unmarshal(cast.StringToBytes(stored), &info); err != nil {
			r.Outcome, r.Err = InitRetryableError, err
			return r
		}
		if info.StreamType == kind {
			oldStmt, err := xsql.Language.Parse(xsql.NewParser(strings.NewReader(info.Statement)))
			if err != nil {
				r.Outcome, r.Err = InitRetryableError, err
				return r
			}
			old, ok := oldStmt.(*ast.StreamStmt)
			if !ok || old.Options == nil {
				r.Outcome, r.Err = InitRetryableError, fmt.Errorf("stored %s %s is invalid", r.Kind, name)
				return r
			}
			if source.Options.SHARED != old.Options.SHARED {
				r.Err = fmt.Errorf("cannot change %s %s SHARED option", r.Kind, name)
				return r
			}
			if !CanReplace(old.Options.VERSION, source.Options.VERSION) {
				r.Outcome = InitSkipped
				return r
			}
		}
	}
	r.Outcome = InitRetryableError
	r.Err = rs.s.execSaveWithPersist(source, statement, true, func(write func() error) error {
		r.Attempts, err = rs.retryInitApply(write)
		return err
	})
	if r.Err == nil {
		r.Outcome = InitApplied
	}
	return r
}

func (rs *RulesetProcessor) applyRule(name, ruleJSON string) InitObjectResult {
	r := InitObjectResult{Kind: "rule", Name: name, Outcome: InitTerminalError, Attempts: 1}
	rule, err := rs.r.GetRuleByJson(name, ruleJSON)
	if err != nil {
		r.Err = err
		return r
	}
	var stored string
	exists, err := rs.r.db.Get(rule.Id, &stored)
	if err != nil {
		r.Outcome, r.Err = InitRetryableError, err
		return r
	}
	if exists {
		old, err := rs.r.GetRuleByJsonValidated(rule.Id, stored)
		if err != nil {
			r.Outcome, r.Err = InitRetryableError, err
			return r
		}
		if !CanReplace(old.Version, rule.Version) {
			r.Outcome = InitSkipped
			return r
		}
	}
	if !rule.Temp {
		r.Attempts, r.Err = rs.retryInitApply(func() error { return rs.r.db.Set(rule.Id, ruleJSON) })
		if r.Err != nil {
			r.Outcome = InitRetryableError
			return r
		}
	}
	r.Outcome = InitApplied
	return r
}

func (rs *RulesetProcessor) retryInitApply(write func() error) (int, error) {
	attempts := 0
	err := backoff.Retry(func() error {
		attempts++
		return write()
	}, rs.newInitBackoff())
	return attempts, err
}

func (rs *RulesetProcessor) ImportRuleSet(all Ruleset) Ruleset {
	ruleSetRsp := Ruleset{
		Rules:   map[string]string{},
		Streams: map[string]string{},
		Tables:  map[string]string{},
	}

	_ = rs.s.streamStatusDb.Clean()
	_ = rs.s.tableStatusDb.Clean()
	_ = rs.r.ruleStatusDb.Clean()

	counts := make([]int, 3)
	// restore streams
	for k, v := range all.Streams {
		_, e := rs.s.ExecReplaceStream(k, v, ast.TypeStream)
		if e != nil {
			conf.Log.Errorf("Fail to import stream %s(%s) with error: %v", k, v, e)
			_ = rs.s.streamStatusDb.Set(k, e.Error())
			ruleSetRsp.Streams[k] = e.Error()
			continue
		}
		counts[0]++
	}
	// restore tables
	for k, v := range all.Tables {
		_, e := rs.s.ExecReplaceStream(k, v, ast.TypeTable)
		if e != nil {
			conf.Log.Errorf("Fail to import table %s(%s) with error: %v", k, v, e)
			_ = rs.s.tableStatusDb.Set(k, e.Error())
			ruleSetRsp.Tables[k] = e.Error()
			continue
		}
		counts[1]++
	}
	// restore rules
	for k, v := range all.Rules {
		_, e := rs.r.ExecCreateWithValidation(k, v)
		if e != nil {
			conf.Log.Errorf("Fail to import rule %s(%s) with error: %v", k, v, e)
			_ = rs.r.ruleStatusDb.Set(k, e.Error())
			ruleSetRsp.Rules[k] = e.Error()
			continue
		}
		counts[2]++
	}
	return ruleSetRsp
}
