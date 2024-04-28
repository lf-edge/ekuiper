// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/pkg/async"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

const (
	dataImportAsyncTask = "dataImport"
)

type asyncTaskRequest struct {
	Type   string                 `json:"type"`
	Params map[string]interface{} `json:"params"`
}

type asyncTaskResponse struct {
	TaskID string `json:"id"`
}

func registerAsyncTask(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	req := &asyncTaskRequest{}
	resp := &asyncTaskResponse{}
	if err := json.Unmarshal(b, req); err != nil {
		handleError(w, err, "", logger)
		return
	}
	switch strings.ToLower(req.Type) {
	case strings.ToLower(dataImportAsyncTask):
		taskID, err := handleDataImportAsyncTask(req.Params)
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		resp.TaskID = taskID
	default:
		err = fmt.Errorf("unknown async task type: %v", req.Type)
		handleError(w, err, "", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	jsonResponse(resp, w, logger)
}

func queryAsyncTaskStatus(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("id")
	s, err := async.GlobalAsyncManager.GetTask(taskID)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	jsonResponse(s, w, logger)
}

func asyncTaskHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		registerAsyncTask(w, r)
	case http.MethodGet:
		queryAsyncTaskStatus(w, r)
	}
}

func asyncTaskCancelHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	taskID := vars["id"]
	if err := async.GlobalAsyncManager.CancelTask(taskID); err != nil {
		handleError(w, err, "", logger)
		return
	}
	w.Write([]byte("cancel success"))
	w.WriteHeader(http.StatusOK)
}

func handleDataImportAsyncTask(params map[string]interface{}) (string, error) {
	rsi := &configurationInfo{}
	if err := cast.MapToStruct(params, rsi); err != nil {
		return "", err
	}
	var partial bool
	var stop bool
	sr, ok := params["stop"].(bool)
	if ok {
		stop = sr
	}
	pr, ok := params["partial"].(bool)
	if ok {
		partial = pr
	}
	taskID := generateTaskID(dataImportAsyncTask)
	subCtx, err := async.GlobalAsyncManager.RegisterTask(taskID)
	if err != nil {
		return "", err
	}
	go func() {
		async.GlobalAsyncManager.StartTask(taskID)
		s, err := handleConfigurationImport(subCtx, rsi, partial, stop)
		if err != nil {
			async.GlobalAsyncManager.TaskFailed(taskID, err)
			return
		}
		b, _ := json.Marshal(s)
		async.GlobalAsyncManager.FinishTask(taskID, string(b))
	}()
	return taskID, nil
}

func generateTaskID(taskType string) string {
	return fmt.Sprintf("%v-%v", taskType, time.Now().Unix())
}
