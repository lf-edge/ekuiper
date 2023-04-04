// Copyright 2022 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package conf

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"testing"
)

func TestSourceConfValidate(t *testing.T) {
	var tests = []struct {
		s   *SourceConf
		e   *SourceConf
		err string
	}{
		{
			s: &SourceConf{},
			e: &SourceConf{
				HttpServerIp:   "0.0.0.0",
				HttpServerPort: 10081,
			},
			err: "invalidHttpServerPort:httpServerPort must between 0 and 65535",
		}, {
			s: &SourceConf{
				HttpServerIp: "192.168.0.1",
			},
			e: &SourceConf{
				HttpServerIp:   "192.168.0.1",
				HttpServerPort: 10081,
			},
			err: "invalidHttpServerPort:httpServerPort must between 0 and 65535",
		}, {
			s: &SourceConf{
				HttpServerPort: 99999,
			},
			e: &SourceConf{
				HttpServerIp:   "0.0.0.0",
				HttpServerPort: 10081,
			},
			err: "invalidHttpServerPort:httpServerPort must between 0 and 65535",
		}, {
			s: &SourceConf{
				HttpServerPort: 9090,
				HttpServerTls: &tlsConf{
					Certfile: "certfile",
					Keyfile:  "keyfile",
				},
			},
			e: &SourceConf{
				HttpServerIp:   "0.0.0.0",
				HttpServerPort: 9090,
				HttpServerTls: &tlsConf{
					Certfile: "certfile",
					Keyfile:  "keyfile",
				},
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		err := tt.s.Validate()
		if err != nil && tt.err != err.Error() {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		}
		if !reflect.DeepEqual(tt.s, tt.e) {
			t.Errorf("%d\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.e)
		}
	}
}

func TestRuleOptionValidate(t *testing.T) {
	var tests = []struct {
		s   *api.RuleOption
		e   *api.RuleOption
		err string
	}{
		{
			s: &api.RuleOption{},
			e: &api.RuleOption{},
		},
		{
			s: &api.RuleOption{
				LateTol:            1000,
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: 300000, //5 minutes
				SendError:          true,
				Restart: &api.RestartStrategy{
					Attempts:     0,
					Delay:        1000,
					Multiplier:   1,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
			e: &api.RuleOption{
				LateTol:            1000,
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: 300000, //5 minutes
				SendError:          true,
				Restart: &api.RestartStrategy{
					Attempts:     0,
					Delay:        1000,
					Multiplier:   1,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
		},
		{
			s: &api.RuleOption{
				LateTol:            1000,
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: 300000, //5 minutes
				SendError:          true,
				Restart: &api.RestartStrategy{
					Attempts:     3,
					Delay:        1000,
					Multiplier:   1,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
			e: &api.RuleOption{
				LateTol:            1000,
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: 300000, //5 minutes
				SendError:          true,
				Restart: &api.RestartStrategy{
					Attempts:     3,
					Delay:        1000,
					Multiplier:   1,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
		},
		{
			s: &api.RuleOption{
				LateTol:            1000,
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: 300000, //5 minutes
				SendError:          true,
				Restart: &api.RestartStrategy{
					Attempts:     3,
					Delay:        1000,
					Multiplier:   1.5,
					MaxDelay:     10000,
					JitterFactor: 0.1,
				},
			},
			e: &api.RuleOption{
				LateTol:            1000,
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: 300000, //5 minutes
				SendError:          true,
				Restart: &api.RestartStrategy{
					Attempts:     3,
					Delay:        1000,
					Multiplier:   1.5,
					MaxDelay:     10000,
					JitterFactor: 0.1,
				},
			},
		},
		{
			s: &api.RuleOption{
				LateTol:            1000,
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: 300000, //5 minutes
				SendError:          true,
				Restart: &api.RestartStrategy{
					Attempts:     -2,
					Delay:        0,
					Multiplier:   0,
					MaxDelay:     0,
					JitterFactor: 1.1,
				},
			},
			e: &api.RuleOption{
				LateTol:            1000,
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: 300000, //5 minutes
				SendError:          true,
				Restart: &api.RestartStrategy{
					Attempts:     0,
					Delay:        1000,
					Multiplier:   2,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
			err: "multiple errors",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		err := ValidateRuleOption(tt.s)
		if err != nil && tt.err == "" {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		}
		if !reflect.DeepEqual(tt.s, tt.e) {
			t.Errorf("%d\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.e)
		}
	}
}
