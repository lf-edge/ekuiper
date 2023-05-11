// Copyright 2021-2022 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conf

import (
	"time"

	"github.com/benbjohnson/clock"
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

func GetLocalZone() int {
	if IsTesting {
		return 28800 // default to UTC+8
	} else {
		_, offset := time.Now().Local().Zone()
		return offset
	}
}

// Time related. For Mock
func GetTicker(duration int) *clock.Ticker {
	return Clock.Ticker(time.Duration(duration) * time.Millisecond)
}

func GetTimer(duration int) *clock.Timer {
	return Clock.Timer(time.Duration(duration) * time.Millisecond)
}

func GetTimerByTime(t time.Time) *clock.Timer {
	if IsTesting {
		return Clock.Timer(time.Duration(t.UnixMilli()-GetNowInMilli()) * time.Millisecond)
	} else {
		return Clock.Timer(time.Until(t))
	}
}

func GetNowInMilli() int64 {
	return Clock.Now().UnixMilli()
}

func GetNow() time.Time {
	return Clock.Now()
}
