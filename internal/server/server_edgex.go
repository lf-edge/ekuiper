//go:build edgex

/*
Copyright NetFoundry Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"net"

	"github.com/sirupsen/logrus"

	"github.com/lf-edge/ekuiper/internal/conf"
	edgex_vault "github.com/lf-edge/ekuiper/internal/edgex"
)

func init() {
	newNetListener = newZitifiedNetListener
}

func newZitifiedNetListener(addr string, logger *logrus.Logger) (net.Listener, error) {
	if conf.Config != nil && conf.Config.Basic.EnableOpenZiti == true {
		logger.Info("using ListenMode 'zerotrust'")
		ctx := edgex_vault.AuthenicatedContext(logger)
		serviceName := "edgex.rules-engine"
		return ctx.Listen(serviceName)
	} else {
		return newTcpListener(addr, logger)
	}
}
