package compressor

import (
	"bytes"
	"testing"
)

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
			for _, name := range []string{ZLIB, GZIP, FLATE} {
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
					t.Error("decompressed data should be equal to input data: %s", name)
				}
			}
		})
	}
}
