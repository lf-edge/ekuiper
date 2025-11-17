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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type kvMap struct {
	data map[string]string
}

func (m *kvMap) Set(key string, value string) error {
	m.data[key] = value
	return nil
}

func (m *kvMap) Get(key string) (string, bool, error) {
	v, ok := m.data[key]
	if !ok {
		return "", false, nil
	}
	return v, true, nil
}

func (m *kvMap) Keys() (keys []string, err error) {
	var s []string
	for k := range m.data {
		s = append(s, k)
	}
	return s, nil
}

func initTestManager() *AsyncManager {
	a := &AsyncManager{
		asyncDB:    &kvMap{data: map[string]string{}},
		taskCancel: map[string]context.CancelFunc{},
	}
	return a
}

func TestAsyncManager(t *testing.T) {
	m := initTestManager()
	id := "123"
	taskCtx, err := m.RegisterTask(id)
	require.NoError(t, err)
	require.NoError(t, m.StartTask(id))
	s, err := m.GetTask(id)
	require.NoError(t, err)
	require.Equal(t, TaskRunningStatus, s.Status)
	require.NoError(t, m.CancelTask(id))
	<-taskCtx.Done()
	s, err = m.GetTask(id)
	require.NoError(t, err)
	require.Equal(t, TaskCancelStatus, s.Status)

	e := m.TaskFailed(id, errors.New("err msg"))
	require.NoError(t, e)
	s, err = m.GetTask(id)
	require.NoError(t, err)
	require.Equal(t, TaskErrorStatus, s.Status)
	require.Equal(t, "err msg", s.Message)

	require.NoError(t, m.FinishTask(id, "success"))
	s, err = m.GetTask(id)
	require.NoError(t, err)
	require.Equal(t, TaskFinishStatus, s.Status)
}
