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

package cast

import (
	"testing"
	"time"
)

func TestDateToAndFromMilli(t *testing.T) {
	var tests = []struct {
		m int64
		t time.Time
	}{
		{int64(1579140864913), time.Date(2020, time.January, 16, 2, 14, 24, 913000000, time.UTC)},
		{int64(4913), time.Date(1970, time.January, 1, 0, 0, 4, 913000000, time.UTC)},
		{int64(2579140864913), time.Date(2051, time.September, 24, 4, 1, 4, 913000000, time.UTC)},
		{int64(-1579140864913), time.Date(1919, time.December, 17, 21, 45, 35, 87000000, time.UTC)},
	}
	for i, tt := range tests {
		time := TimeFromUnixMilli(tt.m)
		if !time.Equal(tt.t) {
			t.Errorf("%d time from milli result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.t, time)
		}
		milli := TimeToUnixMilli(tt.t)
		if tt.m != milli {
			t.Errorf("%d time to milli result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.m, milli)
		}
	}
}
