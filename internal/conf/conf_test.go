// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

func TestSourceConfValidate(t *testing.T) {
	tests := []struct {
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
				HttpServerTls: &TlsConf{
					Certfile: "certfile",
					Keyfile:  "keyfile",
				},
			},
			e: &SourceConf{
				HttpServerIp:   "0.0.0.0",
				HttpServerPort: 9090,
				HttpServerTls: &TlsConf{
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
	tests := []struct {
		s   *def.RuleOption
		e   *def.RuleOption
		err string
	}{
		{
			s: &def.RuleOption{
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
			},
			e: &def.RuleOption{
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
			},
		},
		{
			s: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts:     0,
					Delay:        1000,
					Multiplier:   1,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
			e: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts:     0,
					Delay:        1000,
					Multiplier:   1,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
		},
		{
			s: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts:     3,
					Delay:        1000,
					Multiplier:   1,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
			e: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts:     3,
					Delay:        1000,
					Multiplier:   1,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
		},
		{
			s: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts:     3,
					Delay:        1000,
					Multiplier:   1.5,
					MaxDelay:     10000,
					JitterFactor: 0.1,
				},
			},
			e: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts:     3,
					Delay:        1000,
					Multiplier:   1.5,
					MaxDelay:     10000,
					JitterFactor: 0.1,
				},
			},
		},
		{
			s: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(time.Second), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts:     -2,
					Delay:        0,
					Multiplier:   0,
					MaxDelay:     0,
					JitterFactor: 1.1,
				},
			},
			e: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(time.Second), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts:     0,
					Delay:        1000,
					Multiplier:   2,
					MaxDelay:     1000,
					JitterFactor: 0.1,
				},
			},
			err: "invalidRestartMultiplier:restart multiplier must be greater than 0\ninvalidRestartAttempts:restart attempts must be greater than 0\ninvalidRestartDelay:restart delay must be greater than 0\ninvalidRestartMaxDelay:restart maxDelay must be greater than 0\ninvalidRestartJitterFactor:restart jitterFactor must between [0, 1)",
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			err := ValidateRuleOption(tt.s)
			if tt.err == "" {
				assert.NoError(t, err)
				assert.Equal(t, tt.s, tt.e)
			} else {
				assert.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			}
		})
	}
}

func TestSinkConf_Validate(t *testing.T) {
	tests := []struct {
		name    string
		sc      SinkConf
		wantErr error
	}{
		{
			name: "valid config",
			sc: SinkConf{
				MemoryCacheThreshold: 1024,
				MaxDiskCache:         1024000,
				BufferPageSize:       256,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       0,
			},
			wantErr: nil,
		},
		{
			name: "invalid memoryCacheThreshold",
			sc: SinkConf{
				MemoryCacheThreshold: -1,
				MaxDiskCache:         1024000,
				BufferPageSize:       256,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       0,
			},
			wantErr: errors.Join(errors.New("memoryCacheThreshold:memoryCacheThreshold must be positive")),
		},
		{
			name: "invalid maxDiskCache",
			sc: SinkConf{
				MemoryCacheThreshold: 1024,
				MaxDiskCache:         -1,
				BufferPageSize:       256,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       0,
			},
			wantErr: errors.Join(errors.New("maxDiskCache:maxDiskCache must be positive")),
		},
		{
			name: "invalid bufferPageSize",
			sc: SinkConf{
				MemoryCacheThreshold: 1024,
				MaxDiskCache:         1024000,
				BufferPageSize:       0,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       0,
			},
			wantErr: errors.Join(errors.New("bufferPageSize:bufferPageSize must be positive")),
		},
		{
			name: "invalid resendInterval",
			sc: SinkConf{
				MemoryCacheThreshold: 1024,
				MaxDiskCache:         1024000,
				BufferPageSize:       256,
				EnableCache:          true,
				ResendInterval:       cast.DurationConf(-time.Second),
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       0,
			},
			wantErr: errors.Join(errors.New("resendInterval:resendInterval must be positive")),
		},
		{
			name: "memoryCacheThresholdTooSmall",
			sc: SinkConf{
				MemoryCacheThreshold: 128,
				MaxDiskCache:         1024000,
				BufferPageSize:       256,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       0,
			},
			wantErr: errors.Join(errors.New("memoryCacheThresholdTooSmall:memoryCacheThreshold must be greater than or equal to bufferPageSize")),
		},
		{
			name: "memoryCacheThresholdNotMultiple",
			sc: SinkConf{
				MemoryCacheThreshold: 300,
				MaxDiskCache:         1024000,
				BufferPageSize:       256,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       0,
			},
			wantErr: errors.Join(errors.New("memoryCacheThresholdNotMultiple:memoryCacheThreshold must be a multiple of bufferPageSize")),
		},
		{
			name: "maxDiskCacheTooSmall",
			sc: SinkConf{
				MemoryCacheThreshold: 1024,
				MaxDiskCache:         128,
				BufferPageSize:       256,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       0,
			},
			wantErr: errors.Join(errors.New("maxDiskCacheTooSmall:maxDiskCache must be greater than bufferPageSize")),
		},
		{
			name: "maxDiskCacheNotMultiple",
			sc: SinkConf{
				MemoryCacheThreshold: 1024,
				MaxDiskCache:         300,
				BufferPageSize:       256,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       0,
			},
			wantErr: errors.Join(errors.New("maxDiskCacheNotMultiple:maxDiskCache must be a multiple of bufferPageSize")),
		},
		{
			name: "invalid resendPriority",
			sc: SinkConf{
				MemoryCacheThreshold: 1024,
				MaxDiskCache:         1024000,
				BufferPageSize:       256,
				EnableCache:          true,
				ResendInterval:       0,
				CleanCacheAtStop:     true,
				ResendAlterQueue:     true,
				ResendPriority:       2,
			},
			wantErr: errors.Join(errors.New("resendPriority:resendPriority must be -1, 0 or 1")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sc.Validate()
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

func TestSyslogConf_Validate(t *testing.T) {
	tests := []struct {
		name    string
		sc      *syslogConf
		wantErr error
	}{
		{
			name: "valid config",
			sc: &syslogConf{
				Enable:  false,
				Network: "udp",
				Address: "localhost:514",
				Tag:     "kuiper",
				Level:   "info",
			},
			wantErr: nil,
		},
		{
			name: "empty config",
			sc:   &syslogConf{},
		},
		{
			name: "invalid level",
			sc: &syslogConf{
				Enable: false,
				Level:  "warning",
			},
			wantErr: errors.New("invalid syslog level: warning"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sc.Validate()
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

func TestLoad(t *testing.T) {
	require.NoError(t, os.Setenv("KUIPER__RULE__RESTARTSTRATEGY__ATTEMPTS", "10"))
	SetupEnv()
	InitConf()
	cpath, err := GetConfLoc()
	require.NoError(t, err)
	LoadConfigFromPath(path.Join(cpath, ConfFileName), &Config)
	require.Equal(t, 10, Config.Rule.RestartStrategy.Attempts)
}

func TestJitterFactor(t *testing.T) {
	b := `{"attempts": 0,
            "delay": 1000,
            "jitterFactor": 0.3,
            "maxDelay": 30000,
            "multiplier": 2}`
	r := &def.RestartStrategy{}
	require.NoError(t, json.Unmarshal([]byte(b), r))
	require.Equal(t, 0.3, r.JitterFactor)
}
