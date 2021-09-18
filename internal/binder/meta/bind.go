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
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/meta"
)

// Bind Must run after function and io bound
func Bind() {
	if err := meta.ReadSourceMetaDir(func(name string) bool {
		s, _ := io.Source(name)
		return s != nil
	}); nil != err {
		conf.Log.Errorf("readSourceMetaDir:%v", err)
	}
	if err := meta.ReadSinkMetaDir(func(name string) bool {
		s, _ := io.Sink(name)
		return s != nil
	}); nil != err {
		conf.Log.Errorf("readSinkMetaDir:%v", err)
	}
	if err := meta.ReadFuncMetaDir(func(name string) bool {
		return function.HasFunctionSet(name)
	}); nil != err {
		conf.Log.Errorf("readFuncMetaDir:%v", err)
	}
	if err := meta.ReadUiMsgDir(); nil != err {
		conf.Log.Errorf("readUiMsgDir:%v", err)
	}
}
