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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"strings"
	"sync"
)

var m = clientManager{
	clientFactory:       make(map[string]ClientFactoryFunc),
	lock:                sync.Mutex{},
	supportedClientType: make([]string, 0),
	shareClientStore:    make(map[string]*clientWrapper),
	singleClientStore:   make(map[string]*clientWrapper),
}

type clientManager struct {
	lock                sync.Mutex
	supportedClientType []string
	clientFactory       map[string]ClientFactoryFunc
	shareClientStore    map[string]*clientWrapper
	singleClientStore   map[string]*clientWrapper
}

func registerClientFactory(clientType string, creatorFunc ClientFactoryFunc) {
	m.lock.Lock()
	m.clientFactory[clientType] = creatorFunc
	m.supportedClientType = append(m.supportedClientType, clientType)
	m.lock.Unlock()
}

func getConnectionSelector(props map[string]interface{}) (err error, ConnectionSelector string) {
	for key, v := range props {
		if strings.EqualFold(key, "connectionSelector") {
			if conVal, ok := v.(string); ok {
				return nil, conVal
			} else {
				return fmt.Errorf("connectionSelector value: %v is not string", v), ""
			}
		}
	}
	return nil, ""
}

func GetConnection(reqId, connectionType string, props map[string]interface{}) (interface{}, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	err, connectSelector := getConnectionSelector(props)
	if err != nil {
		return nil, err
	}
	if connectSelector != "" {
		if cliWpr, found := m.shareClientStore[connectSelector]; found {
			cliWpr.addRef()
			return cliWpr.getInstance(), nil
		}
	}

	clientCreator, ok := m.clientFactory[connectionType]
	if !ok {
		conf.Log.Errorf("can not find clientCreator for connection type : %s. only support %s", connectionType, m.supportedClientType)
		return nil, fmt.Errorf("can not find clientCreator for connection type : %s. only support %s", connectionType, m.supportedClientType)
	}
	ctr := clientCreator()

	if connectSelector != "" {
		selectCfg := &conf.ConSelector{
			ConnSelectorStr: connectSelector,
		}
		if err := selectCfg.Init(); err != nil {
			return nil, err
		}

		cf, err := selectCfg.ReadCfgFromYaml()
		if err != nil {
			return nil, err
		}

		cliWpr, err := NewClientWrapper(ctr, cf)
		if err != nil {
			conf.Log.Errorf("can not create client for connection selector : %s have error %s", connectSelector, err)
			return nil, err
		}

		m.shareClientStore[connectSelector] = cliWpr
		return cliWpr.getInstance(), nil

	} else {
		cliWpr, err := NewClientWrapper(ctr, props)
		if err != nil {
			conf.Log.Errorf("can not create client for connection id : %s have error %s", reqId, err)
			return nil, err
		}

		m.singleClientStore[reqId] = cliWpr
		return cliWpr.getInstance(), nil
	}
}

func ReleaseConnection(reqId string, props map[string]interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()

	err, connectSelector := getConnectionSelector(props)
	if err != nil {
		return
	}
	if connectSelector != "" {
		if v, ok := m.shareClientStore[connectSelector]; ok {
			v.subRef()
			conf.Log.Infof("connection selector: %s ReleaseConnection count %d.", connectSelector, v.refCnt)
			if v.IsRefEmpty() {
				v.clean()
				delete(m.shareClientStore, connectSelector)
			}
		}
	} else {
		if v, ok := m.singleClientStore[reqId]; ok {
			v.clean()
			delete(m.shareClientStore, reqId)
		}
	}
}
