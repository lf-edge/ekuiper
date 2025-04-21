// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAbsolutePath(t *testing.T) {
	tests := []struct {
		r string
		a string
	}{
		{
			r: "etc/services",
			a: "/etc/kuiper/services",
		}, {
			r: "data/",
			a: "/var/lib/kuiper/data/",
		}, {
			r: logDir,
			a: "/var/log/kuiper",
		}, {
			r: "plugins",
			a: "/var/lib/kuiper/plugins",
		},
	}
	for i, tt := range tests {
		aa, err := absolutePath(tt.r)
		if err != nil {
			t.Errorf("error: %v", err)
		} else {
			if !(tt.a == aa) {
				t.Errorf("%d result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.a, aa)
			}
		}
	}
}

func TestGetDataLoc_Funcs(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	root := filepath.Join(wd, "..", "..")
	d, err := GetDataLoc()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "data", "test"), d)
}

func TestPathConfig(t *testing.T) {
	PathConfig.Dirs["etc"] = "/etc/kuiper"
	PathConfig.Dirs["data"] = "/data/kuiper"
	PathConfig.Dirs["log"] = "/log/kuiper"
	PathConfig.Dirs["plugins"] = "/tmp/plugins"

	testcases := []struct {
		dir    string
		expect string
	}{
		{
			dir:    etcDir,
			expect: "/etc/kuiper",
		},
		{
			dir:    dataDir,
			expect: "/data/kuiper",
		},
		{
			dir:    logDir,
			expect: "/log/kuiper",
		},
		{
			dir:    pluginsDir,
			expect: "/tmp/plugins",
		},
		{
			dir:    "etc/source",
			expect: "/etc/kuiper/source",
		},
	}

	for _, tc := range testcases {
		d, err := absolutePath(tc.dir)
		require.NoError(t, err)
		require.Equal(t, tc.expect, d)
	}
}
