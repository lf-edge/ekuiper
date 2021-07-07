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
