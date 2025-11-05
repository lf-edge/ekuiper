// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package path

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
)

func AbsPath(ctx api.StreamContext, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(ctx.GetRootPath(), path)
}

func VerifyFileName(name string) error {
	if strings.Contains(name, "..") || filepath.IsAbs(name) {
		return fmt.Errorf("invalid file name: path traversal or absolute paths are not allowed: %q", name)
	}
	return nil
}

func IsSafeFileComponent(name string) bool {
	// Disallow path separators and parent directory references.
	if strings.Contains(name, "/") || strings.Contains(name, "\\") || strings.Contains(name, "..") || name == "" {
		return false
	}
	return true
}
