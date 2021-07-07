// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/benbjohnson/clock"
	"github.com/lf-edge/ekuiper/pkg/cast"
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
