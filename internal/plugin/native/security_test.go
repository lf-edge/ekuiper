package native

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/plugin"
)

// Invalid plugin names must be rejected by name validation before any
// download or unzip happens: the URI below is intentionally bogus to
// prove the name check runs first.
func TestRegisterRejectsInvalidName(t *testing.T) {
	p := &plugin.IOPlugin{
		Name: "../../evil",
		File: "bogus-not-a-url",
	}
	err := manager.Register(plugin.SOURCE, p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")
}
