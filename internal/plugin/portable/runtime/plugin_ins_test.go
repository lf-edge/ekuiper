package runtime_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime"
)

func TestProcess(t *testing.T) {
	pm := runtime.GetPluginInsManager4Test()
	conf.InitConf()
	dir, _ := os.Getwd()
	p := filepath.Join(dir, "../../../../", "sdk/python/example/pysam/pysam.py")
	meta := &runtime.PluginMeta{
		Name:       "pysam",
		Version:    "1.0.0",
		Language:   "python",
		Executable: p,
	}
	_, err := pm.GetOrStartProcess(meta, runtime.PortbleConf)
	require.NoError(t, err)
	err = pm.Kill("pysam")
	require.NoError(t, err)
}
