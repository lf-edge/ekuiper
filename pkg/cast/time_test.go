// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

	"github.com/stretchr/testify/assert"
)

func TestTimeToAndFromMilli(t *testing.T) {
	tests := []struct {
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
		assert.Equal(t, tt.t, time, "%d time from milli result mismatch:\n\nexp=%#v\n\ngot=%#v", i, tt.t, time)

		milli := TimeToUnixMilli(tt.t)
		assert.Equal(t, tt.m, milli, "%d time to milli result mismatch:\n\nexp=%#v\n\ngot=%#v", i, tt.m, milli)
	}
}

func TestFormatTime(t *testing.T) {
	date := time.Date(2020, time.January, 16, 2, 14, 24, 913000000, time.UTC)
	tests := []struct {
		format  string
		want    string
		wantErr bool
	}{
		{
			format:  "YYYY-MM-dd HH:mm:ssSSS",
			want:    "2020-01-16 02:14:24.913",
			wantErr: false,
		},
		{
			format:  "YYYY-MM-dd T HH:mm:ss",
			want:    "2020-01-16 T 02:14:24",
			wantErr: false,
		},
		{
			format:  "YYY",
			want:    "2020",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := FormatTime(date, tt.format)
		if tt.wantErr {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		d       time.Time
		t       string
		f       string
		wantErr bool
	}{
		{
			time.Date(2020, time.January, 16, 2, 14, 24, 913000000, time.UTC),
			"2020-01-16 02:14:24.913",
			"YYYY-MM-dd HH:mm:ssSSS",
			false,
		},
		{
			time.Date(2020, time.January, 16, 2, 14, 24, 0, time.UTC),
			"2020-01-16 02:14:24",
			"YYYY-MM-dd HH:mm:ss",
			false,
		},
		{
			time.Time{},
			"2020",
			"YYY",
			true,
		},
	}
	for _, tt := range tests {
		date, err := ParseTime(tt.t, tt.f)
		if tt.wantErr {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, tt.d, date)
	}
}

func TestInterfaceToTime(t *testing.T) {
	tests := []struct {
		i       interface{}
		f       string
		want    time.Time
		wantErr bool
	}{
		{
			"2022-04-13 06:22:32.233",
			"YYYY-MM-dd HH:mm:ssSSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			false,
		},
		{
			"2022-04-13 6:22:32.2",
			"YYYY-MM-dd h:m:sS",
			time.Date(2022, time.April, 13, 6, 22, 32, 200000000, time.UTC),
			false,
		},
		{
			"2022-04-13 6:22:32.23",
			"YYYY-MM-dd h:m:sSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 230000000, time.UTC),
			false,
		},
		{
			"2022-04-13 Wed 06:22:32.233",
			"YYYY-MM-dd EEE HH:m:ssSSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			false,
		},
		{
			"2022-04-13 Wednesday 06:22:32.233",
			"YYYY-MM-dd EEEE HH:m:ssSSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			false,
		},
		{
			1649830952233,
			"YYYY-MM-dd HH:mm:ssSSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			false,
		},
		{
			int64(1649830952233),
			"YYYY-MM-dd HH:mm:ssSSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			false,
		},
		{
			float64(1649830952233),
			"YYYY-MM-dd HH:mm:ssSSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			false,
		},
		{
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			"YYYY-MM-dd HH:mm:ssSSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			false,
		},
		{
			"2022-04-13 06:22:32.233",
			"YYYy-MM-dd HH:mm:ssSSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			true,
		},
		{
			struct{}{},
			"YYYY-MM-dd HH:mm:ssSSS",
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			true,
		},
	}
	for _, tt := range tests {
		got, err := InterfaceToTime(tt.i, tt.f)
		if tt.wantErr {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}
}

func TestInterfaceToUnixMilli(t *testing.T) {
	tests := []struct {
		i       interface{}
		f       string
		want    int64
		wantErr bool
	}{
		{
			"2022-04-13 06:22:32.233",
			"YYYY-MM-dd HH:mm:ssSSS",
			1649830952233,
			false,
		},
		{
			1649830952233,
			"YYYY-MM-dd HH:mm:ssSSS",
			1649830952233,
			false,
		},
		{
			int64(1649830952233),
			"YYYY-MM-dd HH:mm:ssSSS",
			1649830952233,
			false,
		},
		{
			float64(1649830952233),
			"YYYY-MM-dd HH:mm:ssSSS",
			1649830952233,
			false,
		},
		{
			time.Date(2022, time.April, 13, 6, 22, 32, 233000000, time.UTC),
			"YYYY-MM-dd HH:mm:ssSSS",
			1649830952233,
			false,
		},
		{
			"2022-04-13 06:22:32.233",
			"YYYy-MM-dd HH:mm:ssSSS",
			1649830952233,
			true,
		},
		{
			struct{}{},
			"YYYY-MM-dd HH:mm:ssSSS",
			1649830952233,
			true,
		},
	}
	for _, tt := range tests {
		got, err := InterfaceToUnixMilli(tt.i, tt.f)
		if tt.wantErr {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}
}
