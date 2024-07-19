// Copyright 2023 EMQ Technologies Co., Ltd.
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

package hidden

import (
	"net/url"
)

const (
	PASSWORD = "******"
)

var hiddenPasswdKey map[string]struct{}

func init() {
	hiddenPasswdKey = map[string]struct{}{
		"password": {},
		"token":    {},
	}
}

func ReplacePasswd(resource, config map[string]interface{}) map[string]interface{} {
	for key := range hiddenPasswdKey {
		if hiddenPasswd, ok := config[key]; ok && hiddenPasswd == PASSWORD {
			if passwd, ok := resource[key]; ok {
				if _, ok := passwd.(string); ok {
					config[key] = passwd
				}
			}
		}
	}
	return config
}

func ReplaceUrl(resource, config map[string]interface{}) map[string]interface{} {
	if urlRaw, ok := config["url"]; ok {
		if urlS, ok := urlRaw.(string); ok {
			if u, err := url.Parse(urlS); err == nil {
				if passwd, set := u.User.Password(); set && passwd == PASSWORD {
					if resourceUrl, ok := resource["url"]; ok {
						if r, ok := resourceUrl.(string); ok {
							config["url"] = r
						}
					}
				}
			}
		}
	}
	return config
}
