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

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type RulesetProcessor struct {
	r *RuleProcessor
	s *StreamProcessor
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

// InitImportFailure describes the final outcome of one init.json object.
type InitImportFailure struct {
	Kind      string
	Name      string
	Err       error
	Retryable bool
	Attempts  int
}

type InitImportResult struct {
	Counts   [3]int
	Failures []InitImportFailure
}

func (r InitImportResult) HasRetryableFailure() bool {
	for _, failure := range r.Failures {
		if failure.Retryable {
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
		result.addSource(rs.s, name, statement, ast.TypeStream, 0)
	}
	for name, statement := range all.Tables {
		result.addSource(rs.s, name, statement, ast.TypeTable, 1)
	}
	for name, ruleJSON := range all.Rules {
		if _, err := rs.r.GetRuleByJson(name, ruleJSON); err != nil {
			result.Failures = append(result.Failures, InitImportFailure{Kind: "rule", Name: name, Err: err, Attempts: 1})
			continue
		}
		attempts, err := retryInitCreate(func() error {
			_, e := rs.r.ExecCreateWithValidation(name, ruleJSON)
			return e
		}, func(e error) bool {
			return isStableRuleConflict(rs, name, ruleJSON, e)
		})
		if err != nil {
			result.Failures = append(result.Failures, InitImportFailure{
				Kind: "rule", Name: name, Err: err, Retryable: !isStableRuleConflict(rs, name, ruleJSON, err), Attempts: attempts,
			})
			continue
		}
		result.Counts[2]++
	}
	return result, nil
}

func (r *InitImportResult) addSource(s *StreamProcessor, name, statement string, kind ast.StreamType, countIndex int) {
	if err := validateInitSource(name, statement, kind); err != nil {
		r.Failures = append(r.Failures, InitImportFailure{Kind: ast.StreamTypeMap[kind], Name: name, Err: err, Attempts: 1})
		return
	}
	attempts, err := retryInitCreate(func() error {
		_, e := s.ExecReplaceStream(name, statement, kind)
		return e
	}, func(e error) bool {
		return isStableSourceConflict(e)
	})
	if err != nil {
		r.Failures = append(r.Failures, InitImportFailure{
			Kind: ast.StreamTypeMap[kind], Name: name, Err: err, Retryable: !isStableSourceConflict(err), Attempts: attempts,
		})
		return
	}
	r.Counts[countIndex]++
}

func validateInitSource(name, statement string, kind ast.StreamType) error {
	stmt, err := xsql.Language.Parse(xsql.NewParser(strings.NewReader(statement)))
	if err != nil {
		return err
	}
	source, ok := stmt.(*ast.StreamStmt)
	if !ok || source.StreamType != kind {
		return fmt.Errorf("invalid %s statement", ast.StreamTypeMap[kind])
	}
	if string(source.Name) != name {
		return fmt.Errorf("the SQL statement must create %s %s", ast.StreamTypeMap[kind], name)
	}
	if source.Options == nil {
		return fmt.Errorf("missing options for %s %s", ast.StreamTypeMap[kind], name)
	}
	if source.Options.Temp {
		return fmt.Errorf("cannot initialize %s %s with temp option", ast.StreamTypeMap[kind], name)
	}
	return nil
}

func isStableSourceConflict(err error) bool {
	message := err.Error()
	return strings.Contains(message, "already exists with version") ||
		strings.Contains(message, "do not support to change stream SHARED option")
}

func isStableRuleConflict(rs *RulesetProcessor, name, ruleJSON string, err error) bool {
	if !strings.Contains(err.Error(), "already exists with version") {
		return false
	}
	rule, parseErr := rs.r.GetRuleByJson(name, ruleJSON)
	if parseErr != nil {
		return true
	}
	old, getErr := rs.r.GetRuleById(rule.Id)
	return getErr == nil && !CanReplace(old.Version, rule.Version)
}

func retryInitCreate(create func() error, permanent func(error) bool) (int, error) {
	for attempt := 1; ; attempt++ {
		err := create()
		if err == nil || permanent(err) || attempt == 3 {
			return attempt, err
		}
		time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
	}
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
