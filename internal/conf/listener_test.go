// Copyright 2025 EMQ Technologies Co., Ltd.
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

	ekruntime "github.com/lf-edge/ekuiper/v2/pkg/runtime"
)

func TestListenerConfiguration(t *testing.T) {
	old := Config
	t.Cleanup(func() {
		Config = old
		ekruntime.SetAppConf(old)
		clearLoadConfigCache()
	})
	for _, tc := range []struct {
		name, yaml, want string
		disabled         bool
	}{
		{"omitted", "basic: {}", "127.0.0.1", false},
		{"empty", "basic: {ip: '', restIp: '', prometheusIp: '', pprofIp: ''}\nsource: {httpServerIp: ''}", "127.0.0.1", false},
		{"explicit", "basic: {ip: '192.168.1.10', restIp: '192.168.1.10', prometheusIp: '192.168.1.10', pprofIp: '192.168.1.10', disableRpc: true}\nsource: {httpServerIp: '192.168.1.10'}", "192.168.1.10", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(base, "etc"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(base, "etc", ConfFileName), []byte(tc.yaml), 0o600))
			t.Setenv(KuiperBaseKey, base)
			InitConf()
			require.Equal(t, tc.want, Config.Basic.Ip)
			require.Equal(t, tc.want, Config.Basic.RestIp)
			require.Equal(t, tc.want, Config.Basic.PrometheusIp)
			require.Equal(t, tc.want, Config.Basic.PprofIp)
			require.Equal(t, tc.want, Config.Source.HttpServerIp)
			require.Equal(t, tc.disabled, Config.Basic.DisableRpc)
		})
	}
}
