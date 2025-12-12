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
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/async"
)

const (
	dataImportAsyncTask = "dataImport"
)

type asyncTaskResponse struct {
	TaskID string `json:"id"`
}

func registerDataImportTask(w http.ResponseWriter, r *http.Request) {
	cb := r.URL.Query().Get("stop")
	stop := cb == "1"
	par := r.URL.Query().Get("partial")
	partial := par == "1"
	rsi := &configurationInfo{}
	err := json.NewDecoder(r.Body).Decode(rsi)
	if err != nil {
		handleError(w, err, "Invalid body: Error decoding json", logger)
		return
	}
	taskID, err := handleDataImportAsyncTask(rsi, partial, stop)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	resp := &asyncTaskResponse{}
	resp.TaskID = taskID
	w.WriteHeader(http.StatusOK)
	jsonResponse(resp, w, logger)
}

func queryAsyncTaskStatus(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	taskID := vars["id"]
	s, err := async.GlobalAsyncManager.GetTask(taskID)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	jsonResponse(s, w, logger)
}

func asyncTaskCancelHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	taskID := vars["id"]
	if err := async.GlobalAsyncManager.CancelTask(taskID); err != nil {
		handleError(w, err, "", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("cancel success"))
}

func handleDataImportAsyncTask(rsi *configurationInfo, partial bool, stop bool) (string, error) {
	taskID := generateTaskID(dataImportAsyncTask)
	subCtx, err := async.GlobalAsyncManager.RegisterTask(taskID)
	if err != nil {
		return "", err
	}
	go func() {
		async.GlobalAsyncManager.StartTask(taskID)
		s, err := handleConfigurationImport(subCtx, rsi, partial, stop)
		if err != nil {
			b, _ := json.Marshal(s)
			async.GlobalAsyncManager.TaskFailed(taskID, fmt.Errorf("err:%v, response:%v", err.Error(), string(b)))
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
