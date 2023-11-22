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

package clients

import (
	"fmt"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type clientRegistry struct {
	Lock                sync.Mutex
	supportedClientType []string
	clientFactory       map[string]ClientFactoryFunc
	shareClientStore    map[string]ClientWrapper
}

var gClientRegistry = clientRegistry{
	clientFactory:       make(map[string]ClientFactoryFunc),
	Lock:                sync.Mutex{},
	supportedClientType: make([]string, 0),
	shareClientStore:    make(map[string]ClientWrapper),
}

func RegisterClientFactory(clientType string, creatorFunc ClientFactoryFunc) {
	gClientRegistry.Lock.Lock()
	gClientRegistry.clientFactory[clientType] = creatorFunc
	gClientRegistry.supportedClientType = append(gClientRegistry.supportedClientType, clientType)
	gClientRegistry.Lock.Unlock()
}

func getConnectionSelector(props map[string]interface{}) (ConnectionSelector string, err error) {
	for key, v := range props {
		if strings.EqualFold(key, "connectionSelector") {
			if conVal, ok := v.(string); ok {
				return strings.ToLower(conVal), nil
			} else {
				return "", fmt.Errorf("connectionSelector value: %v is not string", v)
			}
		}
	}
	return "", nil
}

func GetClient(connectionType string, props map[string]interface{}) (api.MessageClient, error) {
	gClientRegistry.Lock.Lock()
	defer gClientRegistry.Lock.Unlock()

	connectSelector, err := getConnectionSelector(props)
	if err != nil {
		return nil, err
	}
	if connectSelector != "" {
		if cliWpr, found := gClientRegistry.shareClientStore[connectSelector]; found {
			cliWpr.AddRef()
			return cliWpr, nil
		}
	}

	clientCreator, ok := gClientRegistry.clientFactory[connectionType]
	if !ok {
		conf.Log.Errorf("can not find clientCreator for connection type : %s. only support %s", connectionType, gClientRegistry.supportedClientType)
		return nil, fmt.Errorf("can not find clientCreator for connection type : %s. only support %s", connectionType, gClientRegistry.supportedClientType)
	}

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
		cliWpr, err := clientCreator(cf)
		if err != nil {
			conf.Log.Errorf("can not create client for connection selector : %s have error %s", connectSelector, err)
			return nil, err
		}
		cliWpr.SetConnectionSelector(connectSelector)
		conf.Log.Infof("Init client wrapper for client type %s and connection selector %s", connectionType, connectSelector)
		gClientRegistry.shareClientStore[connectSelector] = cliWpr
		return cliWpr, nil
	} else {
		cliWpr, err := clientCreator(props)
		if err != nil {
			conf.Log.Errorf("can not create client for cfg : %v have error %s", conf.Printable(props), err)
			return nil, err
		}
		conf.Log.Infof("Init client wrapper for client type %s", connectionType)
		return cliWpr, nil
	}
}

func ReleaseClient(ctx api.StreamContext, cli api.MessageClient) {
	var log api.Logger
	if ctx != nil {
		log = ctx.GetLogger()
	} else {
		log = conf.Log
	}
	wrapper := cli.(ClientWrapper)
	sel := wrapper.GetConnectionSelector()
	ok := wrapper.Release(ctx)

	if sel != "" && ok {
		log.Infof("remove mqtt client wrapper for connection selector %s", sel)
		gClientRegistry.Lock.Lock()
		delete(gClientRegistry.shareClientStore, sel)
		gClientRegistry.Lock.Unlock()
	}
}
