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
	"fmt"
	"strings"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func bumpFrom1TO2() error {
	err := rewriteConfiguration("sources")
	if err != nil {
		return err
	}
	err = rewriteConfiguration("sinks")
	if err != nil {
		return err
	}
	err = rewriteConfiguration("connections")
	return err
}

func rewriteConfiguration(typ string) error {
	failpoint.Inject("rewriteErr", func(val failpoint.Value) {
		switch val.(int) {
		case 1:
			if typ == "sources" {
				failpoint.Return(fmt.Errorf("rewriteErr"))
			}
		case 2:
			if typ == "sinks" {
				failpoint.Return(fmt.Errorf("rewriteErr"))
			}
		case 3:
			if typ == "connections" {
				failpoint.Return(fmt.Errorf("rewriteErr"))
			}
		}
	})

	props, err := conf.GetCfgFromKVStorage(typ, "", "")
	if err != nil {
		return err
	}
	for key, prop := range props {
		t, pluginTyp, id, ok := extractKey(key)
		if !ok {
			continue
		}
		rewrite := false
		switch pluginTyp {
		case "sql":
			rewrite = true
			prop = replaceURL(prop)
		case "influx2":
			rewrite = true
			prop = replaceToken(prop)
		case "kafka":
			rewrite = true
			prop = replaceSaslPassword(prop)
		}
		if rewrite {
			err := conf.WriteCfgIntoKVStorage(t, pluginTyp, id, prop)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func extractKey(key string) (string, string, string, bool) {
	names := strings.Split(key, ".")
	if len(names) != 3 {
		return "", "", "", false
	}
	return names[0], names[1], names[2], true
}

func replaceToken(props map[string]interface{}) map[string]interface{} {
	for key, value := range props {
		if key == "token" {
			props["password"] = value
			delete(props, "token")
			break
		}
	}
	return props
}

func replaceURL(props map[string]interface{}) map[string]interface{} {
	for key, value := range props {
		if key == "url" {
			props["dburl"] = value
			delete(props, "url")
			break
		}
	}
	return props
}

func replaceSaslPassword(prop map[string]interface{}) map[string]interface{} {
	for key, value := range prop {
		if key == "saslPassword" {
			prop["password"] = value
			delete(prop, "saslPassword")
			break
		}
	}
	return prop
}
