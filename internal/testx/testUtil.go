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

package testx

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/sqlkv"
)

// errstring returns the string representation of an error.
func Errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func InitEnv() string {
	conf.InitConf()
	dbDir, err := conf.GetDataLoc()
	if err != nil {
		conf.Log.Fatal(err)
	}
	sqlkv.Setup(dbDir)
	return dbDir
}
