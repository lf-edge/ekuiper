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

func HiddenPassword(kvs map[string]interface{}) map[string]interface{} {
	for k, v := range kvs {
		if m, ok := v.(map[string]interface{}); ok {
			kvs[k] = HiddenPassword(m)
		}
		if _, ok := hiddenPasswdKey[strings.ToLower(k)]; ok {
			kvs[k] = PASSWORD
		}
		if strings.ToLower(k) == "url" {
			if _, ok := v.(string); !ok {
				continue
			}
			u, err := url.Parse(v.(string))
			if err != nil || u.User == nil {
				continue
			}
			password, _ := u.User.Password()
			if password != "" {
				u.User = url.UserPassword(u.User.Username(), PASSWORD)
				kvs[k] = u.String()
			}
		}
	}
	return kvs
}
