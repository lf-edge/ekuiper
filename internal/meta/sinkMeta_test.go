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

package meta

import (
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

func TestHintWhenModifySink(t *testing.T) {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return
	}

	if err = ReadSinkMetaFile(path.Join(confDir, "sinks", "mqtt.json"), true); nil != err {
		t.Error(err)
		return
	}

	showMeta, err := GetSinkMeta("mqtt", "zh_CN")
	if nil != err {
		t.Error(err)
	}

	fmt.Printf("%+v", showMeta)
}

func TestMetaError(t *testing.T) {
	_, err := GetSinkMeta("sql", "123")
	require.Error(t, err)
	ewc, ok := err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())
}
