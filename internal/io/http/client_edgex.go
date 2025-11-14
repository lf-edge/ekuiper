// Copyright 2024 EMQ Technologies Co., Ltd.
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

//go:build edgex || full

package http

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"

	"github.com/openziti/sdk-golang/ziti"
	"github.com/sirupsen/logrus"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	edgex_vault "github.com/lf-edge/ekuiper/v2/internal/edgex"
)

func init() {
	newTransport = newZeroTrustTransport
}

var zitiTransport *http.Transport

type ZitiUnderlayDialer struct{}

var netDialer = &net.Dialer{}

func (d *ZitiUnderlayDialer) Dial(network, address string) (net.Conn, error) {
	return netDialer.Dial(network, address)
}

func newZeroTrustTransport(tlscfg *tls.Config, logger *logrus.Logger) *http.Transport {
	if conf.Config != nil && conf.Config.Basic.EnableOpenZiti {
		logger.Info("using Transport 'zerotrust'")
		// attempt to locate an existing client for this existing token
		if zitiTransport != nil {
			return zitiTransport
		} else {
			ctx := edgex_vault.AuthenicatedContext(logger)

			zitiContexts := ziti.NewSdkCollection()
			zitiContexts.Add(ctx)

			zitiTransport = http.DefaultTransport.(*http.Transport).Clone() // copy default transport
			zitiTransport.TLSClientConfig = tlscfg
			dialer := zitiContexts.NewDialerWithFallback(context.Background(), &ZitiUnderlayDialer{})
			zitiTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dialer.Dial(network, addr)
			}
			return zitiTransport
		}
	} else {
		return getTransport(tlscfg, logger)
	}
}
