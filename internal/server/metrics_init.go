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
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/lf-edge/ekuiper/v2/metrics"
)

func dumpMetricsHandler(w http.ResponseWriter, r *http.Request) {
	startTime, endTime := extractStartEndTime(r)
	zipFilePath, err := metrics.GetMetricsZipFile(startTime, endTime)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	defer os.Remove(zipFilePath)
	downloadHandler(zipFilePath, w, r)
}

func extractStartEndTime(r *http.Request) (time.Time, time.Time) {
	st := r.URL.Query().Get("startTime")
	et := r.URL.Query().Get("endTime")
	sti, err1 := strconv.ParseInt(st, 10, 64)
	eti, err2 := strconv.ParseInt(et, 10, 64)
	if err1 != nil || err2 != nil {
		return time.Now().Add(-1 * time.Hour), time.Now()
	}
	return time.Unix(sti, 0), time.Unix(eti, 0)
}

func downloadHandler(targetFilePath string, w http.ResponseWriter, r *http.Request) {
	if _, err := os.Stat(targetFilePath); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	file, err := os.Open(targetFilePath)
	if err != nil {
		http.Error(w, "Failed to open file", http.StatusInternalServerError)
		return
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		http.Error(w, "Failed to get file info", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filepath.Base(targetFilePath)))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fileInfo.Size()))
	http.ServeContent(w, r, fileInfo.Name(), fileInfo.ModTime(), file)
}
