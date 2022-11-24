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

package schema

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
)

type Info struct {
	Type     def.SchemaType `json:"type"`
	Name     string         `json:"name"`
	Content  string         `json:"content"`
	FilePath string         `json:"file"`
	SoPath   string         `json:"soFile"`
}

func (i *Info) Validate() error {
	if i.Name == "" {
		return fmt.Errorf("name is required")
	}
	if i.Content != "" && i.FilePath != "" {
		return fmt.Errorf("cannot specify both content and file")
	}
	switch i.Type {
	case def.PROTOBUF:
		if i.Content == "" && i.FilePath == "" {
			return fmt.Errorf("must specify content or file")
		}
	case def.CUSTOM:
		if i.SoPath == "" {
			return fmt.Errorf("soFile is required")
		}
	default:
		return fmt.Errorf("unsupported type: %s", i.Type)
	}
	return nil
}

var (
	schemaExt = map[def.SchemaType]string{
		def.PROTOBUF: ".proto",
	}
)
