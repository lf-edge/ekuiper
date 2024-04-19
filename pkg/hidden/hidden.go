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
	"strings"
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

func IsHiddenNecessary(m string) bool {
	lm := strings.ToLower(m)
	if strings.Contains(lm, "url") {
		return true
	}
	for k := range hiddenPasswdKey {
		if strings.Contains(lm, k) {
			return true
		}
	}
	return false
}

func HiddenPassword(kvs map[string]interface{}) map[string]interface{} {
	for k, v := range kvs {
		newV := hiddenPassword4Interface(k, v)
		kvs[k] = newV
	}
	return kvs
}

func hiddenPassword4Interface(k string, v interface{}) interface{} {
	switch vv := v.(type) {
	case map[string]interface{}:
		for kk, vvv := range vv {
			hiddenValue := hiddenPassword4Interface(kk, vvv)
			vv[kk] = hiddenValue
		}
		return vv
	case []interface{}:
		for i, item := range vv {
			vv[i] = hiddenPassword4Interface("", item)
		}
		return vv
	default:
		lk := strings.ToLower(k)
		if _, ok := hiddenPasswdKey[lk]; ok {
			v = PASSWORD
		} else if lk == "url" {
			if _, ok := v.(string); !ok {
				return v
			}
			urlValue, hidden := HiddenURLPasswd(v.(string))
			if hidden {
				v = urlValue
			}
		}
	}
	return v
}

func HiddenURLPasswd(originURL string) (string, bool) {
	u, err := url.Parse(originURL)
	if err != nil || u.User == nil {
		return originURL, false
	}
	password, _ := u.User.Password()
	if password != "" {
		u.User = url.UserPassword(u.User.Username(), PASSWORD)
		n, _ := url.QueryUnescape(u.String())
		return n, true
	}
	return originURL, false
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

func GetHiddenKeys() map[string]struct{} {
	return hiddenPasswdKey
}
