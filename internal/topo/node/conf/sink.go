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

package conf

import (
	"github.com/lf-edge/ekuiper/internal/conf"
)

const ResourceID = "resource_id"

func GetSinkConf(sinkType string, action map[string]interface{}) map[string]interface{} {
	resourceId, ok := action[ResourceID].(string)
	if !ok {
		return action
	}
	delete(action, ResourceID)

	yamlOps, err := conf.NewConfigOperatorFromSinkYaml(sinkType)
	if err != nil {
		conf.Log.Warnf("fail to parse yaml for sink %s. Return error %v", sinkType, err)
		return action
	}
	props := make(map[string]interface{})
	cfg := yamlOps.CopyConfContent()
	if len(cfg) == 0 {
		conf.Log.Warnf("fail to parse yaml for sink %s. Return an empty configuration", sinkType)
		return action
	} else {
		def, ok := cfg[resourceId]
		if !ok {
			conf.Log.Warnf("resource id %s is not found", resourceId)
			return action
		} else {
			props = def
			for k, v := range action {
				props[k] = v
			}
		}
	}

	conf.Log.Debugf("get conf for %s with resource id %s: %v", sinkType, resourceId, printable(props))
	return props
}
