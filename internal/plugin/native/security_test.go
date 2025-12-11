package native

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/plugin"
)

func TestManager_Register_Security_PathTraversal(t *testing.T) {
	s := httptest.NewServer(
		http.FileServer(http.Dir("../testzips")),
	)
	defer s.Close()
	endpoint := s.URL

	tests := []struct {
		name    string
		pName   string
		u       string
		wantErr bool
	}{
		{
			name:    "path traversal attempt",
			pName:   "../../evil",
			u:       endpoint + "/sources/random2.zip",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &plugin.IOPlugin{
				Name: tt.pName,
				File: tt.u,
			}
			err := manager.Register(plugin.SOURCE, p)
			if tt.wantErr {
				if assert.Error(t, err) {
					// Check for the specific validation error
					assert.Contains(t, err.Error(), "path escapes from parent")
				}
			} else {
				assert.NoError(t, err)
				// Cleanup
				manager.Delete(plugin.SOURCE, tt.pName, false)
			}
		})
	}
}
