package mockclock

import (
	"github.com/benbjohnson/clock"
	"github.com/emqx/kuiper/internal/conf"
	"github.com/emqx/kuiper/pkg/cast"
)

func ResetClock(t int64) {
	mock := clock.NewMock()
	mock.Set(cast.TimeFromUnixMilli(t))
	conf.Clock = mock
}

func GetMockClock() *clock.Mock {
	return conf.Clock.(*clock.Mock)
}
