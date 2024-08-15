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

package connection

import (
	"errors"
	"strings"

	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func InitMockTest() {
	conf.IsTesting = true
	modules.ConnectionRegister["mock"] = CreateMockConnection
	modules.ConnectionRegister[strings.ToLower("mockErr")] = CreateMockErrConnection
}

type mockConnection struct {
	id  string
	ref int
}

func (m *mockConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (m *mockConnection) Close(ctx api.StreamContext) error {
	return nil
}

func (m *mockConnection) Attach(ctx api.StreamContext) {
	m.ref++
	return
}

func (m *mockConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
	m.ref--
	return
}

func (m *mockConnection) DetachPub(ctx api.StreamContext, props map[string]any) {
	m.ref--
	return
}

func (m *mockConnection) Ref(ctx api.StreamContext) int {
	return m.ref
}

func CreateMockConnection(ctx api.StreamContext, props map[string]any) (modules.Connection, error) {
	m := &mockConnection{ref: 0}
	return m, nil
}

type mockErrConnection struct{}

func (m mockErrConnection) Ping(ctx api.StreamContext) error {
	return errors.New("mockErr")
}

func (m mockErrConnection) Close(ctx api.StreamContext) {
	return
}

func (m mockErrConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
	return
}

func CreateMockErrConnection(ctx api.StreamContext, props map[string]any) (modules.Connection, error) {
	return nil, backoff.Permanent(errors.New("mockErr"))
}
