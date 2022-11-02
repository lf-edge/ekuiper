// Copyright 2021 EMQ Technologies Co., Ltd.
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

package meta

import (
	"fmt"
	"strings"
)

func GetConnectionMeta(connectionName, language string) (ptrSourceProperty *uiSource, err error) {
	gSourcemetaLock.RLock()
	defer gSourcemetaLock.RUnlock()

	v, found := gSourcemetadata[connectionName+`.json`]
	if !found {
		return nil, fmt.Errorf(`%s%s`, getMsg(language, source, "not_found_plugin"), connectionName)
	}
	ret := make(map[string][]field)
	for kcfg, cfg := range v.ConfKeys {
		var sli []field
		for _, kvs := range cfg {
			if kvs.ConnectionRelated {
				sli = append(sli, kvs)
			}
		}
		ret[kcfg] = sli
	}
	ui := new(uiSource)
	*ui = *v
	ui.ConfKeys = ret
	return ui, nil
}

func GetConnectionPlugins() (sources []*pluginfo) {
	ConfigManager.lock.RLock()
	defer ConfigManager.lock.RUnlock()

	for key, conf := range ConfigManager.cfgOperators {
		if strings.HasPrefix(key, ConnectionCfgOperatorKeyPrefix) {

			plugName := conf.GetPluginName()

			uiSourceRepKey := plugName + `.json`
			gSourcemetaLock.RLock()
			v, found := gSourcemetadata[uiSourceRepKey]
			if !found {
				gSourcemetaLock.RUnlock()
				continue
			}
			gSourcemetaLock.RUnlock()

			node := new(pluginfo)
			node.Name = plugName

			if nil == v.About {
				continue
			}
			node.About = v.About
			i := 0
			for ; i < len(sources); i++ {
				if node.Name <= sources[i].Name {
					sources = append(sources, node)
					copy(sources[i+1:], sources[i:])
					sources[i] = node
					break
				}
			}
			if len(sources) == i {
				sources = append(sources, node)
			}
		}
	}
	return sources
}
