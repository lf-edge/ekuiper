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

package bump

import (
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/replace"
)

func bumpFrom3TO4() error {
	return rewriteReplacedProps()
}

func rewriteReplacedProps() error {
	if err := rewritePlugProps("sources"); err != nil {
		return err
	}
	if err := rewritePlugProps("sinks"); err != nil {
		return err
	}
	if err := rewritePlugProps("connections"); err != nil {
		return err
	}
	return nil
}

func rewritePlugProps(typ string) error {
	keyProps, err := conf.GetCfgFromKVStorage(typ, "", "")
	if err != nil {
		return err
	}
	for key, props := range keyProps {
		_, plug, confKey, valid := extractKey(key)
		if valid {
			changed, newProps := replace.ReplacePropsWithPlug(plug, props)
			if changed {
				return conf.WriteCfgIntoKVStorage(typ, plug, confKey, newProps)
			}
		}
	}
	return nil
}
