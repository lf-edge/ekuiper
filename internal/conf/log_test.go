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

package conf

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogOutdated(t *testing.T) {
	now, err := time.Parse("2006-01-02_15-04-05", "2023-06-29_12-00-00")
	require.NoError(t, err)
	maxDuration := 24 * time.Hour
	testcases := []struct {
		name   string
		remove bool
	}{
		{
			name:   "stream.log",
			remove: false,
		},
		{
			name:   "stream.log.2023-06-20_00-00-00",
			remove: true,
		},
		{
			name:   "stream.log.2023-06-29_00-00-00",
			remove: false,
		},
		{
			name:   "stream.log.2023-error",
			remove: false,
		},
		{
			name:   "rule-demo-1.log.2023-06-20_00-00-00",
			remove: true,
		},
		{
			name:   "rule-demo-2.log.2023-06-29_00-00-00",
			remove: false,
		},
		{
			name:   "rule-demo-3.log.2023-error",
			remove: false,
		},
	}
	for _, tc := range testcases {
		require.Equal(t, tc.remove, isLogOutdated(tc.name, now, maxDuration))
	}
}

func TestValidateLogSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Symlink validation not applicable on Windows")
	}

	// Create temp directory
	tempDir, err := os.MkdirTemp("", "log_symlink_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	linkName := "stream.log"
	linkPath := filepath.Join(tempDir, linkName)

	// Create some rotated log files
	rotated1 := filepath.Join(tempDir, "stream.2023-01-01.log")
	rotated2 := filepath.Join(tempDir, "stream.2023-01-02.log")
	require.NoError(t, os.WriteFile(rotated1, []byte("log1"), 0o644))
	require.NoError(t, os.WriteFile(rotated2, []byte("log2"), 0o644))

	// Ensure rotated2 is newer than rotated1 for deterministic sorting
	now := time.Now()
	require.NoError(t, os.Chtimes(rotated1, now.Add(-1*time.Hour), now.Add(-1*time.Hour)))
	require.NoError(t, os.Chtimes(rotated2, now, now))

	// Test case: no symlink exists, should create one to latest
	err = validateLogSymlink(tempDir, linkName)
	require.NoError(t, err)
	target, err := os.Readlink(linkPath)
	require.NoError(t, err)
	require.Equal(t, rotated2, target) // latest by mod time

	// Test case: symlink exists and points to valid target, should do nothing
	err = validateLogSymlink(tempDir, linkName)
	require.NoError(t, err)

	// Test case: point symlink to non-existent file, should repair
	brokenTarget := filepath.Join(tempDir, "nonexistent.log")
	os.Remove(linkPath)
	require.NoError(t, os.Symlink(brokenTarget, linkPath))
	err = validateLogSymlink(tempDir, linkName)
	require.NoError(t, err)
	target, err = os.Readlink(linkPath)
	require.NoError(t, err)
	require.Equal(t, rotated2, target)

	// Test case: symlink with relative target pointing to existing file
	os.Remove(linkPath)
	relativeTarget := "stream.2023-01-02.log" // relative to link dir
	require.NoError(t, os.Symlink(relativeTarget, linkPath))
	err = validateLogSymlink(tempDir, linkName)
	require.NoError(t, err) // should not repair since target exists

	// Test case: symlink with relative target pointing to non-existent file
	os.Remove(linkPath)
	require.NoError(t, os.Symlink("nonexistent.log", linkPath))
	err = validateLogSymlink(tempDir, linkName)
	require.NoError(t, err)
	target, err = os.Readlink(linkPath)
	require.NoError(t, err)
	require.Equal(t, rotated2, target) // should repair

	// Test case: no accumulated log files, should return nil even if symlink is broken
	// This covers the case where rotatelogs hasn't created any files yet
	os.Remove(rotated1)
	os.Remove(rotated2)
	os.Remove(linkPath)
	require.NoError(t, os.Symlink("nonexistent.log", linkPath))
	err = validateLogSymlink(tempDir, linkName)
	require.NoError(t, err)
	// verify symlink is NOT repaired because no files exist to point to
	target, err = os.Readlink(linkPath)
	require.NoError(t, err)
	require.Equal(t, "nonexistent.log", target)
}
