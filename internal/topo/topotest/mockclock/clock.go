package mockclock

import (
	"github.com/benbjohnson/clock"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func ResetClock(t int64) {
	mock := clock.NewMock()
	mock.Set(cast.TimeFromUnixMilli(t))
	conf.Clock = mock
}

func GetMockClock() *clock.Mock {
	return conf.Clock.(*clock.Mock)
}
