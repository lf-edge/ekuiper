// Copyright 2026 EMQ Technologies Co., Ltd.
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

package converter

import (
	"fmt"
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/converter/delimited"
	"github.com/lf-edge/ekuiper/v2/internal/converter/json"
	"github.com/lf-edge/ekuiper/v2/internal/converter/urlencoded"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

const numCols = 1500

// generateTestData creates a map with numCols columns of numeric values
// Columns are named col0, col1, ..., col1499
// Values alternate between int64 and float64
func generateTestData() map[string]any {
	data := make(map[string]any, numCols)
	for i := 0; i < numCols; i++ {
		key := fmt.Sprintf("col%d", i)
		if i%2 == 0 {
			data[key] = int64(i * 100)
		} else {
			data[key] = float64(i) * 1.5
		}
	}
	return data
}

// generateColNames creates a slice of column names for delimited format
func generateColNames() []string {
	cols := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		cols[i] = fmt.Sprintf("col%d", i)
	}
	return cols
}

// BenchmarkJsonEncode benchmarks JSON format encoding
func BenchmarkJsonEncode(b *testing.B) {
	ctx := mockContext.NewMockContext("benchmark", "json_encode")
	converter := json.NewFastJsonConverter(nil, nil)
	data := generateTestData()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := converter.Encode(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkJsonDecode benchmarks JSON format decoding
func BenchmarkJsonDecode(b *testing.B) {
	ctx := mockContext.NewMockContext("benchmark", "json_decode")
	converter := json.NewFastJsonConverter(nil, nil)
	data := generateTestData()

	// Pre-encode the data
	encoded, err := converter.Encode(ctx, data)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := converter.Decode(ctx, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDelimitedEncode benchmarks delimited/CSV format encoding
func BenchmarkDelimitedEncode(b *testing.B) {
	ctx := mockContext.NewMockContext("benchmark", "delimited_encode")
	converter, err := delimited.NewConverter(map[string]any{
		"delimiter": ",",
		"fields":    generateColNames(),
	})
	if err != nil {
		b.Fatal(err)
	}
	data := generateTestData()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := converter.Encode(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDelimitedDecode benchmarks delimited/CSV format decoding
func BenchmarkDelimitedDecode(b *testing.B) {
	ctx := mockContext.NewMockContext("benchmark", "delimited_decode")
	converter, err := delimited.NewConverter(map[string]any{
		"delimiter": ",",
		"fields":    generateColNames(),
	})
	if err != nil {
		b.Fatal(err)
	}
	data := generateTestData()

	// Pre-encode the data
	encoded, err := converter.Encode(ctx, data)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := converter.Decode(ctx, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUrlencodedEncode benchmarks URL-encoded format encoding
func BenchmarkUrlencodedEncode(b *testing.B) {
	ctx := mockContext.NewMockContext("benchmark", "urlencoded_encode")
	converter, err := urlencoded.NewConverter(nil)
	if err != nil {
		b.Fatal(err)
	}
	data := generateTestData()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := converter.Encode(ctx, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUrlencodedDecode benchmarks URL-encoded format decoding
func BenchmarkUrlencodedDecode(b *testing.B) {
	ctx := mockContext.NewMockContext("benchmark", "urlencoded_decode")
	converter, err := urlencoded.NewConverter(nil)
	if err != nil {
		b.Fatal(err)
	}
	// Use JSON converter to encode first, then decode to get map
	jsonConverter := json.NewFastJsonConverter(nil, nil)
	data := generateTestData()

	// Pre-encode the data using urlencoded converter
	encoded, err := converter.Encode(mockContext.NewMockContext("benchmark", "setup"), data)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))

	// Verify we can decode it
	_, err = converter.Decode(mockContext.NewMockContext("benchmark", "verify"), encoded)
	if err != nil {
		b.Fatal(err)
	}
	_ = jsonConverter // silence unused variable

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := converter.Decode(ctx, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAllFormatsEncode runs all encode benchmarks together for easy comparison
func BenchmarkAllFormatsEncode(b *testing.B) {
	b.Run("Json", BenchmarkJsonEncode)
	b.Run("Delimited", BenchmarkDelimitedEncode)
	b.Run("Urlencoded", BenchmarkUrlencodedEncode)
}

// BenchmarkAllFormatsDecode runs all decode benchmarks together for easy comparison
func BenchmarkAllFormatsDecode(b *testing.B) {
	b.Run("Json", BenchmarkJsonDecode)
	b.Run("Delimited", BenchmarkDelimitedDecode)
	b.Run("Urlencoded", BenchmarkUrlencodedDecode)
}

// TestConverterFormats is a unit test to verify all converters work correctly
func TestConverterFormats(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "formats")
	data := generateTestData()

	testCases := []struct {
		name      string
		converter message.Converter
	}{
		{"json", json.NewFastJsonConverter(nil, nil)},
		{"delimited", mustCreateDelimitedConverter(t)},
		{"urlencoded", mustCreateUrlencodedConverter(t)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test encode
			encoded, err := tc.converter.Encode(ctx, data)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}
			if len(encoded) == 0 {
				t.Fatal("Encoded data is empty")
			}
			t.Logf("%s encoded size: %d bytes", tc.name, len(encoded))

			// Test decode
			decoded, err := tc.converter.Decode(ctx, encoded)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			if decoded == nil {
				t.Fatal("Decoded data is nil")
			}

			// Verify we got a map back
			decodedMap, ok := decoded.(map[string]any)
			if !ok {
				t.Fatalf("Decoded data is not a map, got %T", decoded)
			}
			if len(decodedMap) != numCols {
				t.Errorf("Expected %d columns, got %d", numCols, len(decodedMap))
			}
		})
	}
}

func mustCreateDelimitedConverter(t *testing.T) message.Converter {
	c, err := delimited.NewConverter(map[string]any{
		"delimiter": ",",
		"fields":    generateColNames(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustCreateUrlencodedConverter(t *testing.T) message.Converter {
	c, err := urlencoded.NewConverter(nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
