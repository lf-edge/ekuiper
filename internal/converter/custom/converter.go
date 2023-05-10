// Copyright 2022 EMQ Technologies Co., Ltd.
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

package custom

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter/static"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type Converter struct {
}

var converter = &Converter{}

func LoadConverter(schemaFile string, messageName string, _ string) (message.Converter, error) {
	conf.Log.Infof("Load custom converter from file %s, for symbol Get%s", schemaFile, messageName)
	ffs, err := schema.GetSchemaFile(def.CUSTOM, schemaFile)
	if err != nil {
		return nil, err
	}
	if ffs.SoFile == "" {
		return nil, fmt.Errorf("no so file found for custom schema %s", messageName)
	}
	return static.LoadStaticConverter(ffs.SoFile, messageName)
}
