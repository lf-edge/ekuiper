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
	"net/http"

	"github.com/gorilla/mux"
)

type RuleTagRequest struct {
	Tags []string `json:"tags,omitempty"`
}

type RuleTagResponse struct {
	Rules []string `json:"rules,omitempty"`
}

func updateRuleTags(ruleJson string, tags []string, addOrRemove bool) (string, []string, error) {
	m := make(map[string]any)
	if err := json.Unmarshal([]byte(ruleJson), &m); err != nil {
		return "", nil, err
	}
	ruleTags, ok := m["tags"]
	if !ok {
		m["tags"] = []string{}
		ruleTags = make([]string, 0)
	}
	eTags, ok := ruleTags.([]string)
	var newTags []string
	if ok {
		if addOrRemove {
			newTags = addNewTagsIntoExistTags(tags, eTags)
			m["tags"] = newTags
		} else {
			newTags = removeTagsFromExistTags(tags, eTags)
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
	switch r.Method {
	case http.MethodPut:
		if err := json.NewDecoder(r.Body).Decode(&tagsReq); err != nil {
			handleError(w, err, "decode body error", logger)
			return
		}
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
		if rs.Rule.Tags == nil {
			rs.Rule.Tags = make([]string, 0)
		}
		rs.Rule.Tags = newTags
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
		if rs.Rule.Tags == nil {
			rs.Rule.Tags = make([]string, 0)
		}
		rs.Rule.Tags = newTags
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
	res := make([]string, 0)
	rss := registry.list()
	for _, rs := range rss {
		if rs.Rule.IsTagsMatch(tagsReq.Tags) {
			res = append(res, rs.Rule.Id)
		}
	}
	resp := &RuleTagResponse{Rules: res}
	jsonResponse(resp, w, logger)
}
