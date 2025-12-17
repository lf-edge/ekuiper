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

package fvt

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type UploadTestSuite struct {
	suite.Suite
}

func TestUploadTestSuite(t *testing.T) {
	suite.Run(t, new(UploadTestSuite))
}

func (s *UploadTestSuite) TestUploadPathTraversal() {
	// Test path traversal: attempt to create directory outside upload dir
	targetFile := "../repro_dir/test.txt"
	fileJson := `{"Name": "` + targetFile + `", "Content": "pwned"}`

	uploadDir := filepath.Join(PWD, "data", "uploads")
	parentDir := filepath.Dir(uploadDir)
	reproDirPath := filepath.Join(parentDir, "repro_dir")
	os.RemoveAll(reproDirPath)

	resp, err := client.Post("config/uploads", fileJson)
	s.Require().NoError(err)

	// Should fail due to path traversal protection
	s.Require().Equal(http.StatusBadRequest, resp.StatusCode)

	_, err = os.Stat(reproDirPath)
	s.Require().True(os.IsNotExist(err), "Directory should NOT be created outside upload directory")
}

func (s *UploadTestSuite) TestUploadPathTraversalEmbedded() {
	// Test with embedded .. segments like a/../../../pwned
	targetFile := "a/../../../repro_embedded/test.txt"
	fileJson := `{"Name": "` + targetFile + `", "Content": "pwned"}`

	uploadDir := filepath.Join(PWD, "data", "uploads")
	parentDir := filepath.Dir(uploadDir)
	reproDirPath := filepath.Join(parentDir, "repro_embedded")
	os.RemoveAll(reproDirPath)

	resp, err := client.Post("config/uploads", fileJson)
	s.Require().NoError(err)

	// Should fail due to path traversal protection
	s.Require().Equal(http.StatusBadRequest, resp.StatusCode)

	_, err = os.Stat(reproDirPath)
	s.Require().True(os.IsNotExist(err), "Embedded .. should NOT create directory outside upload")
}

func (s *UploadTestSuite) TestUploadMultiLevelDirectory() {
	// Test multi-level directory creation: a/b/c/file.txt
	// This covers lines 319, 322, 327, 328 (the directory creation loop)
	targetFile := "level1/level2/level3/test.txt"
	fileJson := `{"Name": "` + targetFile + `", "Content": "multi-level test"}`

	uploadDir := filepath.Join(PWD, "data", "uploads")
	nestedPath := filepath.Join(uploadDir, "level1", "level2", "level3")

	// Clean up before test
	os.RemoveAll(filepath.Join(uploadDir, "level1"))

	resp, err := client.Post("config/uploads", fileJson)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	// Verify nested directories were created
	_, err = os.Stat(nestedPath)
	s.Require().NoError(err, "Nested directories should be created")

	// Verify file exists
	filePath := filepath.Join(nestedPath, "test.txt")
	_, err = os.Stat(filePath)
	s.Require().NoError(err, "File should exist in nested directory")

	// Clean up
	os.RemoveAll(filepath.Join(uploadDir, "level1"))

	// Also delete from uploads db
	client.Delete("config/uploads/" + "level1/level2/level3/test.txt")
}

func (s *UploadTestSuite) TestUploadWithLeadingSlash() {
	// Test path with leading slash (empty first part) to cover line 322
	targetFile := "subdir/file.txt"
	fileJson := `{"Name": "` + targetFile + `", "Content": "subdir test"}`

	uploadDir := filepath.Join(PWD, "data", "uploads")

	// Clean up before test
	os.RemoveAll(filepath.Join(uploadDir, "subdir"))

	resp, err := client.Post("config/uploads", fileJson)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)

	// Verify directory and file were created
	filePath := filepath.Join(uploadDir, "subdir", "file.txt")
	_, err = os.Stat(filePath)
	s.Require().NoError(err, "File should exist in subdir")

	// Clean up
	os.RemoveAll(filepath.Join(uploadDir, "subdir"))
	client.Delete("config/uploads/" + "subdir/file.txt")
}
