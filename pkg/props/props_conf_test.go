// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package props

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPropsConf(t *testing.T) {
	err := os.Setenv("KUIPER_PROPS_VAR1", "value1")
	require.NoError(t, err)
	defer os.Unsetenv("KUIPER_PROPS_VAR1")

	err = os.Setenv("KUIPER_PROPS_VIN", "value2")
	require.NoError(t, err)
	defer os.Unsetenv("KUIPER_PROPS_VIN")

	InitProps()
	v1, ok := SC.Get("var1")
	require.True(t, ok)
	require.Equal(t, "value1", v1)
	_, ok = SC.Get("var2")
	require.False(t, ok)
	vin, ok := SC.Get("vin")
	require.True(t, ok)
	require.Equal(t, "value2", vin)
	sf, ok := SC.Get("snowflake")
	require.True(t, ok)
	require.Equal(t, 19, len(sf))
}
