package conf

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// TestSetConsoleAndFileLog_SymlinkValidationFailure triggers the warning when validateLogSymlink fails.
// It creates a regular file where the symlink should be, causing os.Readlink to fail.
func TestSetConsoleAndFileLog_SymlinkValidationFailure(t *testing.T) {
	// Setup temp log dir
	tempDir, err := os.MkdirTemp("", "log_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Mock PathConfig to point logDir to tempDir
	originalDirs := PathConfig.Dirs
	PathConfig.Dirs = map[string]string{
		logDir: tempDir,
	}
	defer func() {
		PathConfig.Dirs = originalDirs
	}()

	// Setup Config
	originalConfig := Config
	Config = &model.KuiperConf{}
	Config.Basic.RotateTime = 1
	Config.Basic.RotateSize = 1024
	Config.Basic.MaxAge = 1
	defer func() {
		Config = originalConfig
	}()

	// Create a regular file named "stream.log" to block symlink creation/validation
	// This will cause validateLogSymlink to fail at os.Readlink (invalid argument) or similar
	logPath := filepath.Join(tempDir, "stream.log")
	require.NoError(t, os.WriteFile(logPath, []byte("conflict"), 0o644))

	// Call SetConsoleAndFileLog
	// Should not return error, but log a warning (which we verify by successful execution)
	err = SetConsoleAndFileLog(false, true)
	require.NoError(t, err)
}

// TestGcOutdatedLog_RemovalFailure triggers the error log when os.Remove fails.
func TestGcOutdatedLog_RemovalFailure(t *testing.T) {
	// Setup temp dir
	tempDir, err := os.MkdirTemp("", "gc_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create an outdated log file
	outdatedName := "stream.log.2000-01-01_00-00-00"
	outdatedPath := filepath.Join(tempDir, outdatedName)
	require.NoError(t, os.WriteFile(outdatedPath, []byte("data"), 0o644))

	// Make the parent directory read-only to prevent removal
	require.NoError(t, os.Chmod(tempDir, 0o500)) // Read + Execute, No Write
	defer os.Chmod(tempDir, 0o755)               // Restore for cleanup

	// Call gcOutdatedLog
	gcOutdatedLog(tempDir, time.Hour)
	// Execution should complete without panic, logging error internally
}

// TestGcOutdatedLog_ReadDirFailure trigger ReadDir error
// by passing a non-existent directory
func TestGcOutdatedLog_ReadDirFailure(t *testing.T) {
	gcOutdatedLog("/non/existent/path/for/gc", time.Hour)
}
