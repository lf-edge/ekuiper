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

package edgex

import (
	"os"

	zitisdk "github.com/openziti/sdk-golang/edge-apis"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/sirupsen/logrus"
)

func AuthenicatedContext(logger *logrus.Logger) ziti.Context {
	adaptLogging(logger)
	edgexCredentialName := os.Getenv("EDGEX_CREDENTIAL_NAME")
	if edgexCredentialName == "" {
		edgexCredentialName = "rules-engine"
	}

	ozController := os.Getenv("OPENZITI_CONTROLLER")
	openZitiRootUrl := "https://" + ozController
	caPool, caErr := ziti.GetControllerWellKnownCaPool(openZitiRootUrl)
	if caErr != nil {
		logger.Panic(caErr)
	}

	sp := SecretProvider(logger)

	credentials := zitisdk.NewJwtCredentials(sp.Jwt())
	credentials.CaPool = caPool

	cfg := &ziti.Config{
		ZtAPI:       openZitiRootUrl + "/edge/client/v1",
		Credentials: credentials,
	}
	cfg.ConfigTypes = append(cfg.ConfigTypes, "all")

	ctx, ctxErr := ziti.NewContext(cfg)
	if ctxErr != nil {
		logger.Panic(ctxErr)
	}
	if err := ctx.Authenticate(); err != nil {
		logger.Panic(err)
	}
	return ctx
}
