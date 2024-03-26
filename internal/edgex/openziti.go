package edgex

import (
	zitisdk "github.com/openziti/sdk-golang/edge-apis"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/sirupsen/logrus"
	"os"
)

func AuthenicatedContext(logger *logrus.Logger) ziti.Context {
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
