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
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type Info struct {
	Type     string `json:"type" yaml:"type"`
	Name     string `json:"name" yaml:"name"`
	Content  string `json:"content,omitempty" yaml:"content,omitempty"`
	FilePath string `json:"file,omitempty" yaml:"filePath,omitempty"`
	SoPath   string `json:"soFile,omitempty" yaml:"soPath,omitempty"`
	Version  string `json:"version,omitempty" yaml:"version,omitempty"`
}

func (i *Info) InstallScript() string {
	marshal, err := json.Marshal(i)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func (i *Info) Validate() error {
	if i.Name == "" {
		return fmt.Errorf("name is required")
	}
	if i.Content != "" && i.FilePath != "" {
		return fmt.Errorf("cannot specify both content and file")
	}
	if _, ok := modules.SchemaTypeDefs[i.Type]; !ok {
		return fmt.Errorf("unsupported schema type %s", i.Type)
	}
	switch i.Type {
	case modules.PROTOBUF:
		if i.Content == "" && i.FilePath == "" {
			return fmt.Errorf("must specify content or file")
		}
	case modules.CUSTOM:
		if i.SoPath == "" {
			return fmt.Errorf("soFile is required")
		}
	}
	return nil
}
