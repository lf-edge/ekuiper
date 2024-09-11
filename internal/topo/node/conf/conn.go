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

package conf

import (
	"errors"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func OverwriteByConnectionConf(connType string, props map[string]interface{}) (map[string]interface{}, error) {
	connSelector, ok := props[ConnectionSelector].(string)
	if !ok {
		return props, nil
	}
	yamlOps, err := conf.NewConfigOperatorFromConnectionStorage(connType)
	failpoint.Inject("overwriteErr", func() {
		err = errors.New("overwriteErr")
	})
	if err != nil {
		return nil, err
	}
	cfg := yamlOps.CopyConfContent()
	connProps, ok := cfg[connSelector]
	if !ok {
		return props, nil
	}
	for k, v := range connProps {
		props[k] = v
	}
	return props, nil
}
