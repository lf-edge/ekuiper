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

package image

import (
	"flag"
	"os"
	"os/exec"
	"os/signal"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestImageWriteFailureCleanup(t *testing.T) {
	const childEnv = "EKUIPER_IMAGE_WRITE_FAILURE_CHILD"
	if os.Getenv(childEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestImageWriteFailureCleanup$", "-test.v")
		// Merge the child's coverage counters into the parent's test profile.
		if coverDir := flag.Lookup("test.gocoverdir"); coverDir != nil && coverDir.Value.String() != "" {
			cmd.Args = append(cmd.Args, "-test.gocoverdir="+coverDir.Value.String())
		}
		cmd.Env = append(os.Environ(), childEnv+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		t.Logf("%s", output)
		return
	}

	// The process-wide file size limit must not affect other package tests.
	// Ignore the signal so the encoder observes EFBIG from the actual write.
	signal.Ignore(unix.SIGXFSZ)
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			fixture := "ekuiper.jpg"
			if format == "png" {
				fixture = "../../../docs/en_US/wechat.png"
			}
			data, err := os.ReadFile(fixture)
			require.NoError(t, err)
			dir := t.TempDir()
			sink := &imageSink{c: &c{Path: dir, ImageFormat: format}}
			err = func() error {
				var original unix.Rlimit
				require.NoError(t, unix.Getrlimit(unix.RLIMIT_FSIZE, &original))
				limited := original
				limited.Cur = 1
				require.NoError(t, unix.Setrlimit(unix.RLIMIT_FSIZE, &limited))
				// Restore before assertions and coverage output write their own files.
				defer func() { require.NoError(t, unix.Setrlimit(unix.RLIMIT_FSIZE, &original)) }()
				return sink.saveFiles(map[string]any{"image": data})
			}()
			require.ErrorIs(t, err, unix.EFBIG)
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, entries, "failed encoding must remove the partial image")
		})
	}
}
