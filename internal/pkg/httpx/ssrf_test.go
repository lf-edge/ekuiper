package httpx

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestReadFileSSRF(t *testing.T) {
	// Start a local test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "secret data")
	}))
	defer server.Close()

	// 1. Defaut behavior: should BLOCK private access
	// Current behavior: ReadFile checks internal/conf.Config.Basic.EnablePrivateNet
	// Since conf.Config is nil or default, it blocks (we should ensure it blocks if nil too, or we mock it)

	// Ensure config is nil or clean to start
	conf.Config = nil

	_, err := ReadFile(server.URL)
	assert.Error(t, err, "ReadFile should block access to local server by default")
	if err != nil {
		assert.Contains(t, err.Error(), "internal network")
	}

	// 2. Enable private access: should ALLOW
	conf.Config = &model.KuiperConf{}
	conf.Config.Basic.EnablePrivateNet = true

	// Reset config after test
	defer func() { conf.Config = nil }()

	rc, err := ReadFile(server.URL)
	assert.NoError(t, err, "ReadFile should allow access when EnablePrivateNet is true")
	if rc != nil {
		rc.Close()
	}
}
