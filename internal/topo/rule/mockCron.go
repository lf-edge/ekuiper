// Copyright 2023 EMQ Technologies Co., Ltd.
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

package rule

import "github.com/robfig/cron/v3"

type MockCron struct {
	index int
}

func (m *MockCron) Start() {}

// AddFunc
// MockCron execute function immediately at once
func (m *MockCron) AddFunc(_ string, cmd func()) (cron.EntryID, error) {
	cmd()
	m.index++
	return cron.EntryID(m.index), nil
}

func (m *MockCron) Remove(id cron.EntryID) {}
