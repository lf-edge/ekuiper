// Copyright 2023-2025 EMQ Technologies Co., Ltd.
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
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

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
					Attempts: 0,
				},
			},
			e: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts: 0,
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
					Attempts: 3,
				},
			},
			e: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts: 3,
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
					Attempts: 3,
				},
			},
			e: &def.RuleOption{
				LateTol:            cast.DurationConf(time.Second),
				Concurrency:        1,
				BufferLength:       1024,
				CheckpointInterval: cast.DurationConf(5 * time.Minute), // 5 minutes
				SendError:          true,
				RestartStrategy: &def.RestartStrategy{
					Attempts: 3,
				},
			},
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

func TestLoad(t *testing.T) {
	require.NoError(t, os.Setenv("KUIPER__RULE__RESTARTSTRATEGY__ATTEMPTS", "10"))
	SetupEnv()
	InitConf()
	cpath, err := GetConfLoc()
	require.NoError(t, err)
	LoadConfigFromPath(path.Join(cpath, ConfFileName), &Config)
	require.Equal(t, 10, Config.Rule.RestartStrategy.Attempts)
}
