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
	"reflect"
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

func TestFormatTime(t *testing.T) {
	type args struct {
		time time.Time
		f    string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				time: time.Date(2020, time.January, 16, 2, 14, 24, 913000000, time.UTC),
				f:    "YYYY-MM-dd HH:mm:ssSSS",
			},
			want:    "2020-01-16 02:14:24.913",
			wantErr: false,
		},
		{
			name: "test1",
			args: args{
				time: time.Date(2020, time.January, 16, 2, 14, 24, 913000000, time.UTC),
				f:    "YYYY-MM-dd T HH:mm:ss",
			},
			want:    "2020-01-16 T 02:14:24",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FormatTime(tt.args.time, tt.args.f)
			if (err != nil) != tt.wantErr {
				t.Errorf("FormatTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FormatTime() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInterfaceToTime(t *testing.T) {
	type args struct {
		i      interface{}
		format string
	}
	tests := []struct {
		name    string
		args    args
		want    time.Time
		wantErr bool
	}{
		{
			name: "test string",
			args: args{
				i:      "2022-04-13 06:22:32.233",
				format: "YYYY-MM-dd HH:mm:ssSSS",
			},
			want:    time.Now(),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InterfaceToTime(tt.args.i, tt.args.format)
			if (err != nil) != tt.wantErr {
				t.Errorf("InterfaceToTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InterfaceToTime() got = %v, want %v", got, tt.want)
			}
		})
	}
}
