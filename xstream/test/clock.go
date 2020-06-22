package test

import (
	"github.com/benbjohnson/clock"
	"github.com/emqx/kuiper/common"
)

func ResetClock(t int64) {
	mock := clock.NewMock()
	mock.Set(common.TimeFromUnixMilli(t))
	common.Clock = mock
}

func GetMockClock() *clock.Mock {
	return common.Clock.(*clock.Mock)
}
