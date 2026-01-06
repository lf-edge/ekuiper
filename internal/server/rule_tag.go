// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"net/http"
	"path"

	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
)

type RuleTagRequest struct {
	Tags []string `json:"tags,omitempty"`
}

type RuleTagResponse struct {
	Rules []string `json:"rules,omitempty"`
}

func resetRuleTags(ruleJson string, newTags []string) (string, error) {
	m := make(map[string]any)
	if err := json.Unmarshal([]byte(ruleJson), &m); err != nil {
		return "", err
	}
	m["tags"] = newTags
	v, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func updateRuleTags(ruleJson string, tags []string, addOrRemove bool) (string, []string, error) {
	m := make(map[string]any)
	if err := json.Unmarshal([]byte(ruleJson), &m); err != nil {
		return "", nil, err
	}
	ruleTags, ok := m["tags"]
	if !ok {
		m["tags"] = [][]interface{}{}
		ruleTags = make([]interface{}, 0)
	}
	tmpTags, ok := ruleTags.([]interface{})
	var newTags []string
	if ok {
		etags := make([]string, 0)
		for _, tag := range tmpTags {
			etags = append(etags, tag.(string))
		}
		if addOrRemove {
			newTags = addNewTagsIntoExistTags(tags, etags)
			m["tags"] = newTags
		} else {
			newTags = removeTagsFromExistTags(tags, etags)
			m["tags"] = newTags
		}
	}
	v, err := json.Marshal(m)
	if err != nil {
		return "", nil, err
	}
	return string(v), newTags, nil
}

func addNewTagsIntoExistTags(newTags []string, existTags []string) []string {
	mTags := make(map[string]struct{})
	for _, tag := range existTags {
		mTags[tag] = struct{}{}
	}
	for _, tag := range newTags {
		_, ok := mTags[tag]
		if !ok {
			existTags = append(existTags, tag)
		}
	}
	return existTags
}

func removeTagsFromExistTags(rTags []string, existTags []string) []string {
	mTags := make(map[string]struct{})
	for _, tag := range rTags {
		mTags[tag] = struct{}{}
	}
	res := make([]string, 0)
	for _, tag := range existTags {
		_, ok := mTags[tag]
		if !ok {
			res = append(res, tag)
		}
	}
	return res
}

func ruleTagHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ruleID := vars["name"]
	defer r.Body.Close()
	tagsReq := &RuleTagRequest{Tags: []string{}}
	if err := json.NewDecoder(r.Body).Decode(&tagsReq); err != nil {
		handleError(w, err, "decode body error", logger)
		return
	}
	switch r.Method {
	case http.MethodPut:
		ruleJson, err := ruleProcessor.GetRuleJson(ruleID)
		if err != nil {
			handleError(w, err, "Get rule error", logger)
			return
		}
		rs, ok := registry.load(ruleID)
		if !ok || rs == nil {
			handleError(w, err, "Get rule error", logger)
			return
		}
		newRuleJson, err := resetRuleTags(ruleJson, tagsReq.Tags)
		if err != nil {
			handleError(w, err, "update rule labels error", logger)
			return
		}

		rule := rs.GetRule()
		newRule := *rule
		newRule.Tags = tagsReq.Tags
		rs.SetRule(&newRule)

		if err := registry.update(ruleID, newRuleJson, rs); err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPatch:
		ruleJson, err := ruleProcessor.GetRuleJson(ruleID)
		if err != nil {
			handleError(w, err, "Get rule error", logger)
			return
		}
		rs, ok := registry.load(ruleID)
		if !ok || rs == nil {
			handleError(w, err, "Get rule error", logger)
			return
		}
		newRuleJson, newTags, err := updateRuleTags(ruleJson, tagsReq.Tags, true)
		if err != nil {
			handleError(w, err, "update rule labels error", logger)
			return
		}

		rule := rs.GetRule()
		newRule := *rule
		newRule.Tags = newTags
		rs.SetRule(&newRule)

		if err := registry.update(ruleID, newRuleJson, rs); err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		ruleJson, err := ruleProcessor.GetRuleJson(ruleID)
		if err != nil {
			handleError(w, err, "Get rule error", logger)
			return
		}
		rs, ok := registry.load(ruleID)
		if !ok || rs == nil {
			handleError(w, err, "Get rule error", logger)
			return
		}
		newRuleJson, newTags, err := updateRuleTags(ruleJson, tagsReq.Tags, false)
		if err != nil {
			handleError(w, err, "update rule labels error", logger)
			return
		}

		rule := rs.GetRule()
		newRule := *rule
		newRule.Tags = newTags
		rs.SetRule(&newRule)

		if err := registry.update(ruleID, newRuleJson, rs); err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func rulesTagsHandler(w http.ResponseWriter, r *http.Request) {
	tagsReq := &RuleTagRequest{Tags: []string{}}
	if err := json.NewDecoder(r.Body).Decode(&tagsReq); err != nil {
		handleError(w, err, "decode body error", logger)
		return
	}
	kv, err := ruleProcessor.GetAllRulesJson()
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	res := make([]string, 0)
	for ruleID, ruleJson := range kv {
		rr, err := ruleProcessor.GetRuleByJsonValidated(ruleID, ruleJson)
		if err != nil {
			continue
		}
		if rr.IsTagsMatch(tagsReq.Tags) {
			res = append(res, ruleID)
		}
	}
	resp := &RuleTagResponse{Rules: res}
	jsonResponse(resp, w, logger)
}

func rulesBulkOperationsHandler(w http.ResponseWriter, r *http.Request) {
	tags := &RuleTagRequest{Tags: []string{}}
	if err := json.NewDecoder(r.Body).Decode(tags); err != nil {
		handleError(w, err, "decode body error", logger)
		return
	}

	rules, err := ruleProcessor.GetAllRulesJson()
	if err != nil {
		handleError(w, err, "", logger)
		return
	}

	resultSet := make([]string, 0)
	for ruleID, ruleJson := range rules {
		rule, err := ruleProcessor.GetRuleByJsonValidated(ruleID, ruleJson)
		if err != nil {
			continue
		}

		if rule.IsTagsMatch(tags.Tags) {
			resultSet = append(resultSet, ruleID)
		}
	}

	if len(resultSet) == 0 {
		handleError(w, errors.New(""), "no matching rules", logger)
		return
	}

	for _, ruleID := range resultSet {
		if _, ok := registry.load(ruleID); !ok {
			ruleJson := rules[ruleID]
			rr, err := ruleProcessor.GetRuleByJsonValidated(ruleID, ruleJson)
			if err != nil {
				continue
			}
			rs := rule.NewState(rr, func(id string, b bool) {
				registry.updateTrigger(id, b)
			})
			registry.register(ruleID, rs)
		}
	}

	for index, ruleID := range resultSet {
		var err error
		if path.Base(r.URL.Path) == "bulkstart" {
			err = registry.StartRule(ruleID)
		} else {
			err = registry.StopRule(ruleID)
		}

		if err != nil {
			handleError(
				w,
				err,
				fmt.Sprintf("operation failed, completed %d operations out of %d", index, len(resultSet)),
				logger,
			)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("operation completed"))
}
