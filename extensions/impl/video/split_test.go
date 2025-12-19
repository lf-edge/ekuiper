// Copyright 2024 EMQ Technologies Co., Ltd.
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

package video

import (
	"reflect"
	"testing"
)

func TestSplitJPEGs(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		atEOF   bool
		wantAdv int
		wantTok []byte
		wantErr bool
	}{
		{
			name:    "empty data at EOF",
			data:    []byte{},
			atEOF:   true,
			wantAdv: 0,
			wantTok: nil,
			wantErr: false,
		},
		{
			name:    "no SOI found, skip all",
			data:    []byte{0x00, 0x01, 0x02},
			atEOF:   false,
			wantAdv: 3,
			wantTok: nil,
			wantErr: false,
		},
		{
			name:    "SOI in the middle",
			data:    []byte{0x00, 0xFF, 0xD8, 0x01},
			atEOF:   false,
			wantAdv: 1,
			wantTok: nil,
			wantErr: false,
		},
		{
			name:    "Valid JPEG",
			data:    []byte{0xFF, 0xD8, 0xAA, 0xBB, 0xFF, 0xD9, 0xCC},
			atEOF:   false,
			wantAdv: 6,
			wantTok: []byte{0xFF, 0xD8, 0xAA, 0xBB, 0xFF, 0xD9},
			wantErr: false,
		},
		{
			name:    "Incomplete JPEG at EOF",
			data:    []byte{0xFF, 0xD8, 0xAA},
			atEOF:   true,
			wantAdv: 3,
			wantTok: nil,
			wantErr: false,
		},
		{
			name:    "Incomplete JPEG not at EOF",
			data:    []byte{0xFF, 0xD8, 0xAA},
			atEOF:   false,
			wantAdv: 0,
			wantTok: nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAdv, gotTok, err := splitJPEGs(tt.data, tt.atEOF)
			if (err != nil) != tt.wantErr {
				t.Errorf("splitJPEGs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotAdv != tt.wantAdv {
				t.Errorf("splitJPEGs() gotAdv = %v, want %v", gotAdv, tt.wantAdv)
			}
			if !reflect.DeepEqual(gotTok, tt.wantTok) {
				t.Errorf("splitJPEGs() gotTok = %v, want %v", gotTok, tt.wantTok)
			}
		})
	}
}
