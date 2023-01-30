// Copyright 2023 EMQ Technologies Co., Ltd.
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

//go:build test

package can

import (
	"testing"

	"github.com/lf-edge/ekuiper/internal/io/mock"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestSource(t *testing.T) {
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"data": []byte{
			0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00,
		}}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"data": []byte{
			0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00,
		}}, nil),
		api.NewDefaultSourceTuple(map[string]interface{}{"data": []byte{
			0x03, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00,
		}}, nil),
	}
	s := &source{}
	err := s.Configure("new", map[string]interface{}{ // the default mock
		"network": "udp",
		"address": "239.64.142.206:49369",
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mock.TestSourceOpen(s, exp, t)
}
