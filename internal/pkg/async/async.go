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

package async

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

const (
	TaskRegisterStatus = "register"
	TaskRunningStatus  = "running"
	TaskFinishStatus   = "finish"
	TaskErrorStatus    = "error"
	TaskCancelStatus   = "cancel"
)

var GlobalAsyncManager *AsyncManager

func InitManager() error {
	var err error
	GlobalAsyncManager = &AsyncManager{
		taskCancel: make(map[string]context.CancelFunc),
	}
	s, err := store.GetKV("asyncManager")
	if err != nil {
		return err
	}
	GlobalAsyncManager.asyncDB = &asyncKV{
		m: s,
	}
	if err != nil {
		return err
	}
	tasks, err := GlobalAsyncManager.GetTaskIDList()
	if err != nil {
		return err
	}
	for _, t := range tasks {
		s, err := GlobalAsyncManager.GetTask(t)
		if err != nil {
			return err
		}
		if s.Status == TaskRunningStatus {
			GlobalAsyncManager.CancelTask(t)
		}
	}
	return nil
}

type AsyncManager struct {
	syncx.RWMutex
	asyncDB    kvInterface
	taskCancel map[string]context.CancelFunc
}

func (m *AsyncManager) GetTaskIDList() ([]string, error) {
	m.RLock()
	defer m.RUnlock()
	return m.asyncDB.Keys()
}

func (m *AsyncManager) GetTask(taskID string) (*AsyncTaskStatus, error) {
	m.RLock()
	defer m.RUnlock()
	return m.getTaskStatus(taskID)
}

func (m *AsyncManager) RegisterTask(taskID string) (context.Context, error) {
	m.Lock()
	defer m.Unlock()
	exists, err := m.isTaskExists(taskID)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("async task: %v not found", taskID)
	}
	n := time.Now().Unix()
	s := &AsyncTaskStatus{
		Status:           TaskRegisterStatus,
		TaskID:           taskID,
		CreatedTimestamp: n,
		UpdatedTimestamp: n,
	}
	if err := m.storeTaskStatus(taskID, s); err != nil {
		return nil, err
	}
	taskCtx, cancel := context.WithCancel(context.Background())
	m.taskCancel[taskID] = cancel
	return taskCtx, nil
}

func (m *AsyncManager) CancelTask(taskID string) error {
	m.Lock()
	defer m.Unlock()
	s, err := m.getTaskStatus(taskID)
	if err != nil {
		return err
	}
	s.Status = TaskCancelStatus
	if err := m.storeTaskStatus(taskID, s); err != nil {
		return err
	}
	m.taskCancel[taskID]()
	return nil
}

func (m *AsyncManager) StartTask(taskID string) error {
	m.Lock()
	defer m.Unlock()
	s, err := m.getTaskStatus(taskID)
	if err != nil {
		return err
	}
	s.Status = TaskRunningStatus
	return m.storeTaskStatus(taskID, s)
}

func (m *AsyncManager) FinishTask(taskID, msg string) error {
	m.Lock()
	defer m.Unlock()
	s, err := m.getTaskStatus(taskID)
	if err != nil {
		return err
	}
	s.Status = TaskFinishStatus
	s.Message = msg
	return m.storeTaskStatus(taskID, s)
}

func (m *AsyncManager) TaskFailed(taskID string, errMsg error) error {
	m.Lock()
	defer m.Unlock()
	s, err := m.getTaskStatus(taskID)
	if err != nil {
		return err
	}
	s.Status = TaskErrorStatus
	s.Message = errMsg.Error()
	return m.storeTaskStatus(taskID, s)
}

func (m *AsyncManager) isTaskExists(taskID string) (bool, error) {
	_, exists, err := m.asyncDB.Get(taskID)
	if err != nil {
		return false, err
	}
	return exists, nil
}

type AsyncTaskStatus struct {
	TaskID           string `json:"id"`
	Status           string `json:"status"`
	Message          string `json:"message"`
	CreatedTimestamp int64  `json:"createdTimestamp"`
	UpdatedTimestamp int64  `json:"updatedTimestamp"`
}

func (m *AsyncManager) getTaskStatus(taskID string) (*AsyncTaskStatus, error) {
	status, exists, err := m.asyncDB.Get(taskID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("async task: %v not found", taskID)
	}
	s := &AsyncTaskStatus{}
	if err := json.Unmarshal([]byte(status), s); err != nil {
		return nil, err
	}
	return s, nil
}

func (m *AsyncManager) storeTaskStatus(taskID string, taskStatus *AsyncTaskStatus) error {
	n := time.Now()
	taskStatus.UpdatedTimestamp = n.Unix()
	v, err := json.Marshal(taskStatus)
	if err != nil {
		return err
	}
	return m.asyncDB.Set(taskID, string(v))
}

type kvInterface interface {
	Set(key string, value string) error
	Get(key string) (string, bool, error)
	Keys() (keys []string, err error)
}

type asyncKV struct {
	m kv.KeyValue
}

func (k *asyncKV) Set(key string, value string) error {
	return k.m.Set(key, value)
}

func (k *asyncKV) Get(key string) (string, bool, error) {
	var s string
	exists, err := k.m.Get(key, &s)
	if err != nil {
		return "", false, err
	}
	if !exists {
		return "", false, nil
	}
	return s, true, nil
}

func (k *asyncKV) Keys() (keys []string, err error) {
	return k.m.Keys()
}
