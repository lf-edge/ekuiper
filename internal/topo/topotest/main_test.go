// Copyright 2026 EMQ Technologies Co., Ltd.
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

package topotest

import (
	"os"
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

// Unit tests exercise legacy file paths (repo fixtures outside the test
// data dir), mirroring the commercial default of external file access on.
// Deny behavior is covered by targeted sandbox tests that toggle the
// switch explicitly.
func TestMain(m *testing.M) {
	if conf.Config == nil {
		conf.InitConf()
	}
	conf.Config.Basic.AllowExternalFileAccess = true
	os.Exit(m.Run())
}
