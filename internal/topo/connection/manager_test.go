package connection

import (
	"reflect"
	"testing"
)

const TestClient = "MockClient"

var GetCalledNumber int
var CloseCalledNumber int

type MockClient struct {
	selector *ConSelector

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
	registerClientFactory("mqtt", func(super *ConSelector) Client {
		return &MockClient{selector: super}
	})

	conSelector := "mqtt.localConnection"

	connection, err := GetConnection(conSelector)
	if err != nil {
		t.Errorf("GetConnection Error")
	}
	value := connection.(string)
	if !reflect.DeepEqual(value, TestClient) {
		t.Errorf("Error")
	}

	connection, err = GetConnection(conSelector)
	if err != nil {
		t.Errorf("GetConnection Error")
	}

	wrapper := m.clientMap[conSelector]
	if !reflect.DeepEqual(wrapper.refCnt, uint32(2)) {
		t.Errorf("Error, ectual=%v, want=%v", wrapper.refCnt, 2)
	}

	connection, err = GetConnection(conSelector)
	if err != nil {
		t.Errorf("GetConnection Error")
	}
	if !reflect.DeepEqual(wrapper.refCnt, uint32(3)) {
		t.Errorf("Error")
	}

	ReleaseConnection(conSelector)
	if !reflect.DeepEqual(wrapper.refCnt, uint32(2)) {
		t.Errorf("Error")
	}

	ReleaseConnection(conSelector)
	if !reflect.DeepEqual(wrapper.refCnt, uint32(1)) {
		t.Errorf("Error")
	}

	ReleaseConnection(conSelector)
	if !reflect.DeepEqual(wrapper.refCnt, uint32(0)) {
		t.Errorf("Error")
	}

	if _, ok := m.clientMap[conSelector]; ok {
		t.Errorf("Error")
	}

	if !reflect.DeepEqual(GetCalledNumber, CloseCalledNumber) || !reflect.DeepEqual(GetCalledNumber, 1) {
		t.Errorf("Error")
	}
}
