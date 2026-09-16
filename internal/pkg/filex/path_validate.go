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
// to use, which may be the canonicalized (symlink-resolved) spelling: the
// validated canonical identity is deliberately kept as the subsequent-use
// path instead of dropping it back to the un-resolved spelling, so the same
// file is never addressed by two different paths downstream. This narrows
// the symlink-alias surface; it does not eliminate TOCTOU between
// validation and use, which remains the caller's trust-domain assumption
// (local write access to the data directory is already fully trusted).
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
	// Physical containment: base and target go through the same resolver
	// (canonicalize longest existing prefix, rejoin missing suffix), so
	// the comparison is always canonical-against-canonical. This keeps
	// working when the base itself does not exist yet but an ancestor is
	// a symlink. Either side failing to resolve degrades to the lexical
	// gate above, never to a wider allowance.
	canonicalBase, err := resolveSymlinks(absDataDir)
	if err != nil {
		return abs, nil
	}
	resolved, err := resolveSymlinks(abs)
	if err != nil {
		return abs, nil
	}
	if err := CheckUnderDir(canonicalBase, resolved); err != nil {
		return "", err
	}
	// Return the canonical spelling so the same file is never addressed
	// by two different paths downstream. This keeps the validated
	// canonical identity as the subsequent-use path instead of dropping
	// it back to the un-resolved spelling.
	return resolved, nil
}

// ValidateFileName ensures a datasource-style file name cannot escape its
// base directory on its own (absolute path or .. elements). The caller must
// additionally validate the joined result with ValidateFilePath.
// When external file access is allowed, this check is skipped: legacy
// relative paths are validated as a whole after joining.
func ValidateFileName(name string) error {
	if name == "" {
		return nil
	}
	if ExternalFileAccessAllowed() {
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
