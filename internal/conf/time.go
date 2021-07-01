package conf

import (
	"github.com/benbjohnson/clock"
	"github.com/emqx/kuiper/pkg/cast"
	"time"
)

var Clock clock.Clock

func InitClock() {
	if IsTesting {
		Log.Debugf("running in testing mode")
		Clock = clock.NewMock()
	} else {
		Clock = clock.New()
	}
}

//Time related. For Mock
func GetTicker(duration int) *clock.Ticker {
	return Clock.Ticker(time.Duration(duration) * time.Millisecond)
}

func GetTimer(duration int) *clock.Timer {
	return Clock.Timer(time.Duration(duration) * time.Millisecond)
}

func GetNowInMilli() int64 {
	return cast.TimeToUnixMilli(Clock.Now())
}
