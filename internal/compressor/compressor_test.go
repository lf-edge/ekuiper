// Copyright 2023 carlclone@gmail.com.
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

package compressor

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func BenchmarkCompressor(b *testing.B) {
	compressors := []string{ZLIB, GZIP, FLATE, ZSTD}

	data, err := ioutil.ReadFile("test.json")
	if err != nil {
		b.Fatalf("failed to read test file: %v", err)
	}

	for _, c := range compressors {
		b.Run(c, func(b *testing.B) {
			wc, err := GetCompressor(c)
			firstCompressedData, err := wc.Compress(data)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {

				compressedData, err := wc.Compress(data)
				if err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(firstCompressedData, compressedData) {
					b.Errorf("decompressed data should be equal to input data: %s", c)
				}
			}
		})
	}
}

func BenchmarkDecompressor(b *testing.B) {
	compressors := []string{ZLIB, GZIP, FLATE, ZSTD}

	data, err := ioutil.ReadFile("test.json")
	if err != nil {
		b.Fatalf("failed to read test file: %v", err)
	}

	for _, c := range compressors {
		wc, err := GetCompressor(c)
		if err != nil {
			b.Fatal(err)
		}
		compressedData, err := wc.Compress(data)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(c, func(b *testing.B) {
			de, err := GetDecompressor(c)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decompressedData, err := de.Decompress(compressedData)
				if err != nil {
					b.Fatal(err)
				}

				if !bytes.Equal(data, decompressedData) {
					b.Errorf("decompressed data should be equal to input data: %s", c)
				}
			}
		})
	}
}

func TestCompressionRatio(t *testing.T) {
	// Load JSON file
	data, err := ioutil.ReadFile("test.json")
	if err != nil {
		t.Fatalf("failed to read test file: %v", err)
	}

	compressors := []string{ZLIB, GZIP, FLATE, ZSTD}

	for _, c := range compressors {
		wc, err := GetCompressor(c)
		if err != nil {
			t.Fatal(err)
		}
		compressed, err := wc.Compress(data)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("%s Compression ratio: %f\n", c, float64(len(data))/float64(len(compressed)))
	}
}

func TestGetCompressor(t *testing.T) {
	testCases := []struct {
		name          string
		compressor    string
		expectedError bool
	}{
		{
			name:          "valid compressor zlib",
			compressor:    "zlib",
			expectedError: false,
		},
		{
			name:          "valid compressor gzip",
			compressor:    "gzip",
			expectedError: false,
		},
		{
			name:          "valid compressor flate",
			compressor:    "flate",
			expectedError: false,
		},
		{
			name:          "valid compressor zsstd",
			compressor:    "zstd",
			expectedError: false,
		},
		{
			name:          "unsupported compressor",
			compressor:    "invalid",
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compr, err := GetCompressor(tc.compressor)
			if tc.expectedError && err == nil {
				t.Errorf("expected error but got nil")
			}
			if !tc.expectedError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.expectedError && compr == nil {
				t.Errorf("expected non-nil compressor but got nil")
			}
		})
	}
}

func TestCompressAndDecompress(t *testing.T) {
	testCases := []struct {
		name      string
		inputData []byte
	}{
		{
			name:      "compress/decompress a simple string",
			inputData: []byte("Hello, world!"),
		},
		{
			name:      "compress/decompress a larger data set",
			inputData: bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x04}, 10000),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range []string{ZLIB, GZIP, FLATE, ZSTD} {
				compr, err := GetCompressor(name)
				if err != nil {
					t.Fatalf("get compressor failed: %v", err)
				}
				compressedData, err := compr.Compress(tc.inputData)
				if err != nil {
					t.Fatalf("unexpected error while compressing data: %v", err)
				}
				if len(compressedData) == 0 {
					t.Error("compressed data should not be empty")
				}

				decompr, err := GetDecompressor(name)
				if err != nil {
					t.Fatalf("get decompressor failed: %v", err)
				}
				decompressedData, err := decompr.Decompress(compressedData)
				if err != nil {
					t.Fatalf("unexpected error while decompressing data: %v", err)
				}
				if !bytes.Equal(tc.inputData, decompressedData) {
					t.Errorf("decompressed data should be equal to input data: %s", name)
				}
			}
		})
	}
}
