// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package timex

import (
	"os"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
)

var (
	Clock     clock.Clock
	IsTesting bool
	Maxtime   = time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)
)

func init() {
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.") {
			IsTesting = true
			break
		}
	}
	InitClock()
}

func InitClock() {
	if IsTesting {
		Clock = clock.NewMock()
	} else {
		Clock = clock.New()
	}
}

// GetTicker Time related. For Mock
func GetTicker(duration time.Duration) *clock.Ticker {
	return Clock.Ticker(duration)
}

func After(duration time.Duration) <-chan time.Time {
	return Clock.After(duration)
}

func GetTimer(duration time.Duration) *clock.Timer {
	return Clock.Timer(duration)
}

func Sleep(duration time.Duration) {
	Clock.Sleep(duration)
}

func GetTimerByTime(t time.Time) *clock.Timer {
	if IsTesting {
		return Clock.Timer(t.Sub(GetNow()))
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

// Mock time, only use in test

func Set(t int64) {
	if IsTesting {
		Clock.(*clock.Mock).Set(time.UnixMilli(t))
	}
}

func SetNow(t time.Time) {
	if IsTesting {
		Clock.(*clock.Mock).Set(t)
	}
}

func Add(d time.Duration) {
	if IsTesting {
		Clock.(*clock.Mock).Add(d)
	}
}
