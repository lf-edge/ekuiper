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

package protoutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jhump/protoreflect/desc/protoparse" //nolint:staticcheck
)

// CollectFiles returns the .proto files at path. A single file is returned as-is;
// a directory is expanded to its direct .proto children.
func CollectFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{path}, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var result []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".proto") {
			result = append(result, filepath.Join(path, entry.Name()))
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no .proto files found in directory %s", path)
	}
	return result, nil
}

// ResolveFiles converts absolute file paths to names relative to the parser's
// import paths. Relative names are kept as-is for compatibility with callers
// that resolve schemas from the current working directory.
func ResolveFiles(importPaths, protoFiles []string) ([]string, error) {
	validImportPaths := make([]string, 0, len(importPaths))
	for _, importPath := range importPaths {
		if importPath != "" {
			validImportPaths = append(validImportPaths, importPath)
		}
	}
	if len(validImportPaths) == 0 {
		return protoFiles, nil
	}
	for i, protoFile := range protoFiles {
		if !filepath.IsAbs(protoFile) {
			continue
		}
		resolved, err := protoparse.ResolveFilenames(validImportPaths, protoFile)
		if err != nil {
			return nil, err
		}
		protoFiles[i] = resolved[0]
	}
	return protoFiles, nil
}
