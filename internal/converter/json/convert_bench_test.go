// Copyright 2023 EMQ Technologies Co., Ltd.
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

package json

import (
	"os"
	"testing"

	"github.com/lf-edge/ekuiper/pkg/ast"
)

func BenchmarkSimpleTuples(b *testing.B) {
	benchmarkByFiles("./testdata/simple.json", b, nil)
}

func BenchmarkSimpleTuplesWithSchema(b *testing.B) {
	schema := map[string]*ast.JsonStreamField{
		"key": {
			Type: "string",
		},
	}
	benchmarkByFiles("./testdata/simple.json", b, schema)
}

func BenchmarkSmallJSON(b *testing.B) {
	benchmarkByFiles("./testdata/small.json", b, nil)
}

func BenchmarkSmallJSONWithSchema(b *testing.B) {
	schema := map[string]*ast.JsonStreamField{
		"sid": {
			Type: "bigint",
		},
	}
	benchmarkByFiles("./testdata/small.json", b, schema)
}

func BenchmarkMediumJSON(b *testing.B) {
	benchmarkByFiles("./testdata/medium.json", b, nil)
}

func BenchmarkMediumJSONWithSchema(b *testing.B) {
	schema := map[string]*ast.JsonStreamField{
		"person": {
			Type: "struct",
			Properties: map[string]*ast.JsonStreamField{
				"id": {
					Type: "string",
				},
			},
		},
	}
	benchmarkByFiles("./testdata/medium.json", b, schema)
}

func BenchmarkLargeJSON(b *testing.B) {
	benchmarkByFiles("./testdata/large.json", b, nil)
}

func BenchmarkLargeJSONWithSchema(b *testing.B) {
	schema := map[string]*ast.JsonStreamField{
		"users": {
			Type: "array",
			Items: &ast.JsonStreamField{
				Type: "struct",
				Properties: map[string]*ast.JsonStreamField{
					"id": {
						Type: "bigint",
					},
				},
			},
		},
	}
	benchmarkByFiles("./testdata/large.json", b, schema)
}

func BenchmarkComplexTuples(b *testing.B) {
	benchmarkByFiles("./testdata/MDFD.json", b, nil)
}

func BenchmarkComplexTuplesWithSchema(b *testing.B) {
	schema := map[string]*ast.JsonStreamField{
		"STD_AbsoluteWindDirection": {
			Type: "float",
		},
	}
	benchmarkByFiles("./testdata/MDFD.json", b, schema)
}

func benchmarkByFiles(filePath string, b *testing.B, schema map[string]*ast.JsonStreamField) {
	payload, err := os.ReadFile(filePath)
	if err != nil {
		b.Fatalf(err.Error())
	}
	if schema != nil {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			fastConverter.DecodeWithSchema(payload, schema)
		}
	} else {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			converter.Decode(payload)
		}
	}
}
