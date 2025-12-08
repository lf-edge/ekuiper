package runtime

import (
	"sync/atomic"

	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

var (
	initialized atomic.Bool
	conf        *model.KuiperConf
)

func SetAppConf(cfg *model.KuiperConf) {
	conf = cfg
	initialized.Store(true)
}

// GetAppConf foreign module can get app conf from this
func GetAppConf() *model.KuiperConf {
	if !initialized.Load() {
		panic("FATAL: appconfig.Get() called before initialization")
	}
	return conf
}
