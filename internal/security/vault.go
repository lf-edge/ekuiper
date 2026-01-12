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

// Package security provides cryptographic utilities for secure configuration loading.
package security

// GetMasterKey constructs the 32-byte AES-256 key at runtime using byte scattering.
// This prevents the key from appearing as a contiguous string in the binary.
func GetMasterKey() []byte {
	// Scattered byte slices (unmeaningful values)
	part1 := []byte{0xa7, 0x3c, 0x91, 0xd8, 0x4f, 0x2b, 0xe6, 0x15}
	part2 := []byte{0x8d, 0x52, 0xc9, 0x76, 0xf3, 0x0a, 0xb4, 0x6e}
	part3 := []byte{0x1f, 0x83, 0xd7, 0x4a, 0x95, 0x28, 0xec, 0x60}
	part4 := []byte{0x3b, 0xa2, 0x7d, 0xc1, 0x56, 0x89, 0xe4, 0x0f}

	// Assemble key
	key := make([]byte, 32)
	copy(key[0:], part1)
	copy(key[8:], part2)
	copy(key[16:], part3)
	copy(key[24:], part4)

	return key
}

// ClearKey zeroes out the key slice (best effort memory cleanup)
func ClearKey(key []byte) {
	for i := range key {
		key[i] = 0
	}
}
