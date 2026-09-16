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

package filex

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

// ExternalFileAccessAllowed reports whether the global
// allowExternalFileAccess switch is on. A nil config is treated as
// restrictive (fail-closed).
func ExternalFileAccessAllowed() bool {
	return conf.Config != nil && conf.Config.Basic.AllowExternalFileAccess
}

// ValidateFilePath ensures p resolves inside the data directory unless
// external file access is explicitly allowed. When external access is off,
// relative paths are resolved against the data directory; when it is on,
// they resolve against the process working directory (historical behavior)
// and only absolute paths are returned as-is. It returns the absolute path
// to use.
func ValidateFilePath(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("path must be set")
	}
	clean := filepath.Clean(p)
	if ExternalFileAccessAllowed() {
		abs, err := filepath.Abs(clean)
		if err != nil {
			return "", fmt.Errorf("invalid path %s: %v", p, err)
		}
		return abs, nil
	}
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		return "", fmt.Errorf("failed to get data directory: %v", err)
	}
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve data directory: %v", err)
	}
	var abs string
	if filepath.IsAbs(clean) {
		abs = clean
	} else {
		abs = filepath.Join(absDataDir, clean)
	}
	// Lexical gate: lexical target against lexical base.
	if err := CheckUnderDir(absDataDir, abs); err != nil {
		return "", err
	}
	// Symlink re-check: resolve pre-planted symlinks on the existing
	// portion of the path, so data/<link-to-etc>/x cannot escape.
	// Both sides must be canonical here: the data directory itself may
	// live behind a symlink (e.g. /opt/ekuiper-current -> /opt/ekuiper-2.2.0),
	// and comparing a canonical target against a non-canonical base would
	// misclassify legitimate files as outside. If either side cannot be
	// resolved, the lexical gate above stands as the containment.
	canonicalBase := absDataDir
	if c, err := filepath.EvalSymlinks(absDataDir); err == nil {
		canonicalBase = c
	}
	resolved, err := resolveSymlinks(abs)
	if err != nil {
		resolved = abs
	}
	if resolved != abs {
		if err := CheckUnderDir(canonicalBase, resolved); err != nil {
			return "", err
		}
		// Return the canonical spelling so the same file is never
		// addressed by two different paths downstream.
		return resolved, nil
	}
	return abs, nil
}

// ValidateFileName ensures a datasource-style file name cannot escape its
// base directory on its own (absolute path or .. elements). The caller must
// additionally validate the joined result with ValidateFilePath.
func ValidateFileName(name string) error {
	if name == "" {
		return nil
	}
	if filepath.IsAbs(name) {
		return fmt.Errorf("invalid datasource %q: absolute path is not allowed", name)
	}
	clean := filepath.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("invalid datasource %q: path traversal is not allowed", name)
	}
	return nil
}

func CheckUnderDir(base, target string) error {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return fmt.Errorf("file access denied: cannot resolve path %s", target)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("file access denied: path %s is outside the data directory, enable allowExternalFileAccess to allow it", target)
	}
	return nil
}

// resolveSymlinks evaluates symlinks on the longest existing prefix of p.
// It returns p unchanged when nothing exists yet.
func resolveSymlinks(p string) (string, error) {
	if _, err := os.Lstat(p); err == nil {
		return filepath.EvalSymlinks(p)
	}
	dir := filepath.Dir(p)
	for dir != "." && dir != string(os.PathSeparator) {
		if _, err := os.Lstat(dir); err == nil {
			resolved, err := filepath.EvalSymlinks(dir)
			if err != nil {
				return "", err
			}
			rel, err := filepath.Rel(dir, p)
			if err != nil {
				return "", err
			}
			return filepath.Join(resolved, rel), nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return p, nil
}
