// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"reflect"
	"testing"
)

const TestClient = "MockClient"

var GetCalledNumber int
var CloseCalledNumber int

type MockClient struct {
	selector *conf.ConSelector

	mockCon string
}

func (m *MockClient) CfgValidate(m2 map[string]interface{}) error {
	return nil
}

func (m *MockClient) GetClient() (interface{}, error) {
	GetCalledNumber = 1
	m.mockCon = "MockClient"
	return m.mockCon, nil
}

func (m *MockClient) CloseClient() error {
	CloseCalledNumber = 1
	m.mockCon = ""
	return nil
}

func TestManager(t *testing.T) {
	MQTT := "mqtt"
	registerClientFactory(MQTT, func() Client {
		return &MockClient{}
	})

	conSelector := "mqtt.localConnection"
	reqId := "test"
	props := map[string]interface{}{
		"server":             "tcp:127.0.0.1:1883",
		"USERNAME":           "demo",
		"Password":           "password",
		"clientID":           "clientid",
		"connectionSelector": conSelector,
	}

	connection, err := GetConnection(reqId, MQTT, props)
	if err != nil {
		t.Errorf("GetConnection Error")
	}
	value := connection.(string)
	if !reflect.DeepEqual(value, TestClient) {
		t.Errorf("Error")
	}

	connection, err = GetConnection(reqId, MQTT, props)
	if err != nil {
		t.Errorf("GetConnection Error")
	}

	wrapper := m.shareClientStore[conSelector]
	if !reflect.DeepEqual(wrapper.refCnt, uint32(2)) {
		t.Errorf("Error, ectual=%v, want=%v", wrapper.refCnt, 2)
	}

	connection, err = GetConnection(reqId, MQTT, props)
	if err != nil {
		t.Errorf("GetConnection Error")
	}
	if !reflect.DeepEqual(wrapper.refCnt, uint32(3)) {
		t.Errorf("Error")
	}

	ReleaseConnection(reqId, props)
	if !reflect.DeepEqual(wrapper.refCnt, uint32(2)) {
		t.Errorf("Error")
	}

	ReleaseConnection(reqId, props)
	if !reflect.DeepEqual(wrapper.refCnt, uint32(1)) {
		t.Errorf("Error")
	}

	ReleaseConnection(reqId, props)
	if !reflect.DeepEqual(wrapper.refCnt, uint32(0)) {
		t.Errorf("Error")
	}

	if _, ok := m.shareClientStore[conSelector]; ok {
		t.Errorf("Error")
	}

	if !reflect.DeepEqual(GetCalledNumber, CloseCalledNumber) || !reflect.DeepEqual(GetCalledNumber, 1) {
		t.Errorf("Error")
	}
}
