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

package http

import (
	"fmt"
	"net/http"
	"reflect"
	"testing"
)

func TestConfigureLookup(t *testing.T) {
	tests := []struct {
		name        string
		props       map[string]interface{}
		err         error
		config      *RawConf
		accessConf  *AccessTokenConf
		refreshConf *RefreshTokenConf
		tokens      map[string]interface{}
	}{
		{
			name: "default",
			props: map[string]interface{}{
				"url": "http://localhost:9090/",
			},
			config: &RawConf{
				Url:                "http://localhost:9090/",
				ResendUrl:          "http://localhost:9090/",
				Method:             http.MethodGet,
				Interval:           DefaultInterval,
				Timeout:            DefaultTimeout,
				BodyType:           "none",
				ResponseType:       "code",
				InsecureSkipVerify: true,
			},
		},
	}
	server := mockAuthServer()
	server.Start()

	defer server.Close()
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test %d: %s", i, tt.name), func(t *testing.T) {
			r := &lookupSource{}
			err := r.Configure("", tt.props)
			if err != nil {
				if tt.err == nil {
					t.Errorf("Expected error: %v", err)
				} else {
					if err.Error() != tt.err.Error() {
						t.Errorf("Error mismatch\nexp\t%v\ngot\t%v", tt.err, err)
					}
				}
				return
			}
			if !reflect.DeepEqual(r.config, tt.config) {
				t.Errorf("Config mismatch\nexp\t%+v\ngot\t%+v", tt.config, r.config)
			}
			if !reflect.DeepEqual(r.accessConf, tt.accessConf) {
				t.Errorf("AccessConf mismatch\nexp\t%+v\ngot\t%+v", tt.accessConf, r.accessConf)
			}
			if !reflect.DeepEqual(r.refreshConf, tt.refreshConf) {
				t.Errorf("RefreshConf mismatch\nexp\t%+v\ngot\t%+v", tt.refreshConf, r.refreshConf)
			}
			if !reflect.DeepEqual(r.tokens, tt.tokens) {
				t.Errorf("Tokens mismatch\nexp\t%s\ngot\t%s", tt.tokens, r.tokens)
			}
		})
	}
}
