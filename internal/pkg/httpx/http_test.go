// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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

package httpx

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func TestIsUrl(t *testing.T) {
	urls := []string{
		"http://localhost:8080/abc",
		"https://localhost:8080/abc",
		"http://122.122.122:8080/abc",
	}
	for _, u := range urls {
		if err := IsHttpUrl(u); err != nil {
			t.Errorf("expect %s is url but got %v", u, err)
		}
	}
	badUrls := []string{
		"ws://localhost:8080/abc",
		"http:/baidu.com:8080/abc",
		"localhost:8080/abc",
	}
	for _, u := range badUrls {
		if err := IsHttpUrl(u); err == nil {
			t.Errorf("expect %s is not url but passed", u)
		}
	}
}

func TestErr(t *testing.T) {
	tests := []struct {
		name string
		u    string
		data any
		err  string
	}{
		{
			name: "wrong data",
			u:    "http://noexist.org",
			data: 45,
			err:  "http send only supports bytes but receive invalid content: 45",
		},
		{
			name: "wrong url",
			u:    "\\\abc",
			data: "test",
			err:  "fail to create request: parse \"\\\\\\abc\": net/url: invalid control character in URL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := SendWithFormData(conf.Log, nil, "formdata", "POST", test.u, nil, nil, "", test.data)
			require.EqualError(t, err, test.err)
		})
	}
}
