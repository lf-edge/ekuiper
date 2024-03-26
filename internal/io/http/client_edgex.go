// Copyright 2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//go:build edgex

package http

import (
	"context"
	"crypto/tls"
	edgex_vault "github.com/lf-edge/ekuiper/internal/edgex"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/sirupsen/logrus"
	"net"
	"net/http"
)

func init() {
	newTransport = newZeroTrustTransport
}

var zitiTransport *http.Transport

func newZeroTrustTransport(tlscfg *tls.Config, logger *logrus.Logger) *http.Transport {
	// attempt to locate an existing client for this existing token
	if zitiTransport != nil {
		return zitiTransport
	} else {
		ctx := edgex_vault.AuthenicatedContext(logger)

		zitiContexts := ziti.NewSdkCollection()
		zitiContexts.Add(ctx)

		zitiTransport = http.DefaultTransport.(*http.Transport).Clone() // copy default transport
		zitiTransport.TLSClientConfig = tlscfg
		zitiTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := zitiContexts.NewDialer()
			return dialer.Dial(network, addr)
		}
		return zitiTransport
	}
}
