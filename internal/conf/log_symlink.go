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

package conf

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// validateLogSymlink checks if the log symlink points to an existing file and repairs it if not.
// This addresses an issue where rotatelogs may create symlinks that point to deleted files
// when RotateCount is small and rotation causes filename cycling.
// Only runs on non-Windows platforms where symlinks are supported.
func validateLogSymlink(logDir, linkName string) error {
	linkPath := filepath.Join(logDir, linkName)
	target, err := os.Readlink(linkPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Symlink doesn't exist, try to create it to the latest log file
			return repairLogSymlink(logDir, linkName)
		}
		return fmt.Errorf("failed to read symlink %s: %w", linkPath, err)
	}
	// Resolve relative targets against the symlink's directory
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(linkPath), target)
	}
	if _, err := os.Stat(target); os.IsNotExist(err) {
		// Target doesn't exist, repair symlink
		return repairLogSymlink(logDir, linkName)
	}
	return nil
}

// repairLogSymlink finds the latest rotated log file and updates the symlink
func repairLogSymlink(logDir, linkName string) error {
	linkPath := filepath.Join(logDir, linkName)

	// Find all rotated log files
	// Find all rotated log files
	ext := filepath.Ext(linkName)
	prefix := linkName[:len(linkName)-len(ext)]
	pattern := filepath.Join(logDir, prefix+".*"+ext)

	allFiles, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to glob log files in %s: %w", logDir, err)
	}
	if len(allFiles) == 0 {
		// No rotated files, nothing to link to - rotatelogs will handle initial symlink creation
		return nil
	}

	// Filter to only files that can be stat'ed (exclude inaccessible ones)
	var files []string
	for _, f := range allFiles {
		if _, err := os.Stat(f); err == nil {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		return fmt.Errorf("no accessible rotated log files found in %s", logDir)
	}

	// Sort files by modification time, newest first
	sort.Slice(files, func(i, j int) bool {
		infoI, errI := os.Stat(files[i])
		infoJ, errJ := os.Stat(files[j])
		if errI != nil || errJ != nil {
			return false
		}
		if infoI.ModTime().Equal(infoJ.ModTime()) {
			return files[i] > files[j]
		}
		return infoI.ModTime().After(infoJ.ModTime())
	})

	latestFile := files[0]

	// Remove existing symlink if it exists
	if _, err := os.Lstat(linkPath); err == nil {
		if err := os.Remove(linkPath); err != nil {
			return fmt.Errorf("failed to remove existing symlink %s: %w", linkPath, err)
		}
	}

	// Create new symlink
	if err := os.Symlink(latestFile, linkPath); err != nil {
		return fmt.Errorf("failed to create symlink %s -> %s: %w", linkPath, latestFile, err)
	}
	return nil
}
