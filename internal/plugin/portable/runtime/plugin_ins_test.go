package runtime_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime"
)

func TestProcess(t *testing.T) {
	// handshake Error
	t.Skip()
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

func TestProcessErr(t *testing.T) {
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
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime/CreateControlChannelErr", "return(true)")
	_, err := pm.GetOrStartProcess(meta, runtime.PortbleConf)
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime/CreateControlChannelErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime/confErr", "return(true)")
	_, err = pm.GetOrStartProcess(meta, runtime.PortbleConf)
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime/confErr")

	x := "mock"
	_, err = pm.GetOrStartProcess(&runtime.PluginMeta{
		Name:        "pysam",
		Version:     "1.0.0",
		Language:    "python",
		Executable:  p,
		VirtualType: &x,
	}, runtime.PortbleConf)
	require.Error(t, err)

	_, err = pm.GetOrStartProcess(&runtime.PluginMeta{
		Name:       "pysam",
		Version:    "1.0.0",
		Language:   "mock",
		Executable: p,
	}, runtime.PortbleConf)
	require.Error(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime/cmdStartErr", "return(true)")
	_, err = pm.GetOrStartProcess(meta, runtime.PortbleConf)
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime/cmdStartErr")
}
