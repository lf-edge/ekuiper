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

package httpx

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestDownloadFileTLS(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SSL_CERT_FILE trust-store isolation requires Linux")
	}
	// System roots are cached process-wide. Run in a fresh process so this test
	// neither depends on nor changes the trust store used by other tests.
	const childEnv = "EKUIPER_DOWNLOAD_TLS_TEST_CHILD"
	if os.Getenv(childEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestDownloadFileTLS$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		t.Logf("%s", output)
		return
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "download test CA"},
		NotBefore:    now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, publicKey, privateKey)
	require.NoError(t, err)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))
	t.Setenv("SSL_CERT_FILE", caPath)
	t.Setenv("SSL_CERT_DIR", t.TempDir())

	oldConfig := conf.Config
	t.Cleanup(func() { conf.Config = oldConfig })
	conf.Config = &model.KuiperConf{}
	conf.Config.Basic.EnablePrivateNet = true

	certificate := func(wrongHostname, expired bool) tls.Certificate {
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(2),
			NotBefore:    now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			KeyUsage:    x509.KeyUsageDigitalSignature,
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		}
		if wrongHostname {
			leaf.IPAddresses = nil
			leaf.DNSNames = []string{"wrong.invalid"}
		}
		if expired {
			leaf.NotBefore = now.Add(-2 * time.Hour)
			leaf.NotAfter = now.Add(-time.Hour)
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, publicKey, privateKey)
		require.NoError(t, err)
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: privateKey}
	}
	startTLS := func(cert tls.Certificate, handler http.Handler) *httptest.Server {
		server := httptest.NewUnstartedServer(handler)
		server.Config.ErrorLog = log.New(io.Discard, "", 0)
		server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
		server.StartTLS()
		t.Cleanup(server.Close)
		return server
	}
	const body = "synthetic plugin archive"
	var archiveRequests atomic.Int32
	archive := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		archiveRequests.Add(1)
		_, _ = io.WriteString(w, body)
	})
	validCert := certificate(false, false)
	valid := startTLS(validCert, archive)
	wrong := startTLS(certificate(true, false), archive)
	expired := startTLS(certificate(false, true), archive)
	untrusted := httptest.NewUnstartedServer(archive)
	untrusted.Config.ErrorLog = log.New(io.Discard, "", 0)
	untrusted.StartTLS()
	t.Cleanup(untrusted.Close)
	plain := httptest.NewServer(archive)
	t.Cleanup(plain.Close)

	check := func(t *testing.T, uri, wantError string) {
		t.Helper()
		dir := t.TempDir()
		before := archiveRequests.Load()
		downloaded, err := DownloadFile(dir, "plugin.zip", uri)
		if wantError != "" {
			require.ErrorContains(t, err, wantError)
			require.Empty(t, downloaded)
			_, err = os.Stat(filepath.Join(dir, "plugin.zip"))
			require.ErrorIs(t, err, os.ErrNotExist)
			require.Equal(t, before, archiveRequests.Load(), "rejected download must not reach the archive handler")
			return
		}
		require.NoError(t, err)
		data, err := os.ReadFile(downloaded)
		require.NoError(t, err)
		require.Equal(t, body, string(data))
	}
	for _, tc := range []struct{ name, uri, wantError string }{
		{"trusted", valid.URL, ""},
		{"untrusted", untrusted.URL, "unknown authority"},
		{"wrong_hostname", wrong.URL, "certificate"},
		{"expired", expired.URL, "expired"},
		{"direct_http", plain.URL, ""},
	} {
		t.Run(tc.name, func(t *testing.T) { check(t, tc.uri, tc.wantError) })
	}
	for _, status := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		for _, tc := range []struct{ name, uri, wantError string }{
			{"trusted", valid.URL, ""},
			{"untrusted", untrusted.URL, "unknown authority"},
			{"wrong_hostname", wrong.URL, "certificate"},
			{"downgrade", plain.URL, "cannot redirect HTTPS download to http"},
		} {
			t.Run(fmt.Sprintf("redirect_%d/%s", status, tc.name), func(t *testing.T) {
				redirect := startTLS(validCert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, tc.uri, status)
				}))
				check(t, redirect.URL, tc.wantError)
			})
		}
	}
	t.Run("redirect_limit", func(t *testing.T) {
		var requests atomic.Int32
		loop := startTLS(validCert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			http.Redirect(w, r, "/loop", http.StatusFound)
		}))
		check(t, loop.URL, "stopped after 10 redirects")
		require.EqualValues(t, 10, requests.Load())
	})
	t.Run("http_upgrade", func(t *testing.T) {
		upgrade := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, valid.URL, http.StatusFound)
		}))
		defer upgrade.Close()
		check(t, upgrade.URL, "")
	})
	t.Run("http_upgrade_then_downgrade", func(t *testing.T) {
		downgrade := startTLS(validCert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, plain.URL, http.StatusFound)
		}))
		upgrade := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, downgrade.URL, http.StatusFound)
		}))
		defer upgrade.Close()
		check(t, upgrade.URL, "cannot redirect HTTPS download to http")
	})
	t.Run("private_network_blocked", func(t *testing.T) {
		conf.Config.Basic.EnablePrivateNet = false
		defer func() { conf.Config.Basic.EnablePrivateNet = true }()
		check(t, valid.URL, "internal network")
	})
}
