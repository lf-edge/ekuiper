// Copyright 2025 EMQ Technologies Co., Ltd.
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

//go:build parquet || full

package converter

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/converter/parquet"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterConverter(message.FormatParquet, func(_ api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, _ map[string]any) (message.Converter, error) {
		return &parquet.ParquetWriter{}, nil
	})
	modules.RegisterWriterConverter(message.FormatParquet, func(_ api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, _ map[string]any) (message.ConvertWriter, error) {
		return &parquet.ParquetWriter{}, nil
	})
}
