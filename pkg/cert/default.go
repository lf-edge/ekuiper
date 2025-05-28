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

package cert

import (
	"crypto/tls"
	"errors"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// read only
var conf *model.TlsConfigurationOptions

// InitConf run in server start up
func InitConf(tc *model.TlsConfigurationOptions) {
	conf = tc
}

func GetDefaultTlsConf(ctx api.StreamContext) (*tls.Config, error) {
	if conf == nil {
		return nil, errors.New("default TLS is not configured")
	}
	keys, err := conf.GenKeys()
	if err != nil {
		return nil, err
	}
	return GenerateTLSForClient(ctx, conf, keys)
}
