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

func rulesLabelsHandler(w http.ResponseWriter, r *http.Request) {
	labels := make(map[string]string)
	if err := json.NewDecoder(r.Body).Decode(&labels); err != nil {
		handleError(w, err, "decode body error", logger)
		return
	}
	res := make([]string, 0)
	rss := registry.list()
	for _, rs := range rss {
		if rs.Rule.MatchLabels(labels) {
			res = append(res, rs.Rule.Id)
		}
	}
	jsonResponse(res, w, logger)
}

func ruleLabelsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ruleID := vars["name"]
	defer r.Body.Close()
	switch r.Method {
	case http.MethodPost:
		labels := make(map[string]string)
		if err := json.NewDecoder(r.Body).Decode(&labels); err != nil {
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
		newRuleJson, err := addRuleLabels(ruleJson, labels)
		if err != nil {
			handleError(w, err, "update rule labels error", logger)
			return
		}
		if rs.Rule.Labels == nil {
			rs.Rule.Labels = make(map[string]string)
		}
		for k, v := range labels {
			rs.Rule.Labels[k] = v
		}
		if err := registry.save(ruleID, newRuleJson, rs); err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		keys := make([]string, 0)
		if err := json.NewDecoder(r.Body).Decode(&keys); err != nil {
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
		newRuleJson, err := deleteRuleLabels(ruleJson, keys)
		if err != nil {
			handleError(w, err, "update rule labels error", logger)
			return
		}
		if rs.Rule.Labels == nil {
			rs.Rule.Labels = make(map[string]string)
		}
		for _, key := range keys {
			delete(rs.Rule.Labels, key)
		}
		if err := registry.save(ruleID, newRuleJson, rs); err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func addRuleLabels(ruleJson string, labels map[string]string) (string, error) {
	m := make(map[string]any)
	if err := json.Unmarshal([]byte(ruleJson), &m); err != nil {
		return "", err
	}
	ruleLabels, ok := m["labels"]
	if ok {
		mlabel, ok := ruleLabels.(map[string]string)
		if ok {
			for k, v := range labels {
				mlabel[k] = v
			}
			m["labels"] = mlabel
		}
	}
	v, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func deleteRuleLabels(ruleJson string, keys []string) (string, error) {
	m := make(map[string]any)
	if err := json.Unmarshal([]byte(ruleJson), &m); err != nil {
		return "", err
	}
	ruleLabels, ok := m["labels"]
	if ok {
		mlabel, ok := ruleLabels.(map[string]string)
		if ok {
			for _, k := range keys {
				delete(mlabel, k)
			}
		}
	}
	v, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(v), nil
}
