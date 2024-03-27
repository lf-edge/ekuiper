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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type VaultSecret struct {
	scheme          string
	host            string
	port            int16
	secretName      string
	vaultToken      string
	renewalFactor   float64
	client          *http.Client
	authContext     map[string]interface{}
	logger          *logrus.Logger
	renewalCallback func()
	started         bool
	wg              sync.WaitGroup
}

const (
	DEFAULT_EDGEX_SERVICE_NAME = "rules-engine"
	DEFAULT_CREDENTIAL_FILE    = "/tmp/edgex/secrets/rules-engine/secrets-token.json"
	DEFAULT_VAULT_HOST         = "vault"
	DEFAULT_VAULT_PORT         = 8200
)

var vaultSecret *VaultSecret

func SecretProvider(logger *logrus.Logger) *VaultSecret {
	serviceName := os.Getenv("EDGEX_SERVICE_NAME")
	if strings.TrimSpace(serviceName) == "" {
		serviceName = DEFAULT_EDGEX_SERVICE_NAME
	}
	initialSecret := os.Getenv("EDGEX_CREDENTIALS")
	if strings.TrimSpace(initialSecret) == "" {
		initialSecret = DEFAULT_CREDENTIAL_FILE
	}

	vaultHost := os.Getenv("EDGEX_VAULT_HOST")
	if strings.TrimSpace(vaultHost) == "" {
		vaultHost = DEFAULT_VAULT_HOST
	}

	var vaultPort int16
	vaultPortStr := os.Getenv("EDGEX_VAULT_PORT")
	if strings.TrimSpace(vaultPortStr) == "" {
		vaultPort = DEFAULT_VAULT_PORT
	}

	if vaultSecret != nil {
		return vaultSecret
	}
	secretBytes, err := os.ReadFile(initialSecret)
	if err != nil {
		logger.Errorf("could not read initial secret at :%s", initialSecret)
		return nil
	}

	v := &VaultSecret{
		client:        &http.Client{},
		scheme:        "http",
		host:          vaultHost,
		port:          vaultPort,
		secretName:    serviceName,
		renewalFactor: 0.75,
		wg:            sync.WaitGroup{},
		logger:        logger,
	}

	v.renewalCallback = v.autoRenew

	if initSecErr := v.readToken(secretBytes); initSecErr != nil {
		logger.Panicf("exchangeVaultToken failed. error reading initial secret: %v", initSecErr)
	}
	v.Start()
	vaultSecret = v
	return vaultSecret
}

func (v *VaultSecret) Start() {
	if v.started {
		return
	}
	v.started = true
	v.wg.Add(1)
	if exchangeErr := v.exchangeVaultToken(); exchangeErr != nil {
		v.logger.Panicf("exchangeVaultToken failed. error exchanging token: %v", exchangeErr)
	}
	v.wg.Done()
}

func (v *VaultSecret) autoRenew() {
	renewErr := v.renewToken()
	if renewErr != nil {
		v.logger.Panic(renewErr)
	}
}

func (v *VaultSecret) renewToken() error {
	url := fmt.Sprintf("%s://%s:%d/v1/auth/token/renew-self", v.scheme, v.host, v.port)

	req, newReqErr := http.NewRequest(http.MethodPost, url, nil)
	if newReqErr != nil {
		return fmt.Errorf("renewToken failed. error creating request: %v", newReqErr)
	}

	respBody, callErr := v.callVault(req)
	if callErr != nil {
		return callErr
	}
	defer func() { _ = respBody.Close() }()

	body, readErr := io.ReadAll(respBody)
	if readErr != nil {
		return fmt.Errorf("renewToken failed. error reading response body: %v", readErr)
	}

	if readTokenErr := v.readToken(body); readTokenErr != nil {
		return fmt.Errorf("the vault token could not be refreshed! %v", readTokenErr)
	}
	err := v.exchangeVaultToken()
	if err != nil {
		return err
	}
	return nil
}

func (v *VaultSecret) exchangeVaultToken() error {
	url := fmt.Sprintf("%s://%s:%d/v1/identity/oidc/token/"+v.secretName, v.scheme, v.host, v.port)

	req, newReqErr := http.NewRequest(http.MethodGet, url, nil)
	if newReqErr != nil {
		return fmt.Errorf("exchangeVaultToken failed. error creating request: %v", newReqErr)
	}

	respBody, callErr := v.callVault(req)
	if callErr != nil {
		return callErr
	}
	defer func() { _ = respBody.Close() }()

	body, readErr := io.ReadAll(respBody)
	if readErr != nil {
		return fmt.Errorf("exchangeVaultToken failed. error reading response body: %v", readErr)
	}

	var resp map[string]interface{}
	if unmarshalErr := json.Unmarshal(body, &resp); unmarshalErr != nil {
		return fmt.Errorf("exchangeVaultToken failed. error decoding JSON: %v", unmarshalErr)
	}
	v.authContext = resp["data"].(map[string]interface{})

	// using the result, setup a callback schedule to keep the token fresh in case it's ever needed
	ttl := v.authContext["ttl"]
	callbackTime := time.Duration(ttl.(float64) * v.renewalFactor * float64(time.Second))
	v.logger.Infof("vault token valid for %f seconds. token renewal will occur in %v", ttl, callbackTime)

	time.AfterFunc(callbackTime, v.renewalCallback)
	return nil
}

func (v *VaultSecret) readToken(content []byte) error {
	var vaultResponse struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}

	err := json.Unmarshal(content, &vaultResponse)
	if err != nil {
		return fmt.Errorf("exchangeVaultToken failed. could not read vault token: %v", err)
	}

	v.vaultToken = vaultResponse.Auth.ClientToken
	return nil
}

func (v *VaultSecret) callVault(req *http.Request) (io.ReadCloser, error) {
	req.Header.Set("X-Vault-Token", v.vaultToken)
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return resp.Body, fmt.Errorf("error making request, http status code not OK: %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (v *VaultSecret) Jwt() string {
	return v.authContext["token"].(string)
}
