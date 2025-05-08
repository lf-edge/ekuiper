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

package fvt

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type PortableTestSuite struct {
	suite.Suite
}

func TestPortableTestSuite(t *testing.T) {
	suite.Run(t, new(PortableTestSuite))
}

func (s *PortableTestSuite) TestLC() {
	streamSql := `{"sql": "create stream pyjsonStream () WITH (TYPE=\"pyjson\", FORMAT=\"json\")"}`
	ruleSql := `{
	  "id": "rulePort1",
	  "sql": "SELECT * FROM pyjsonStream",
	  "actions": [
		{
		  "print": {
            "requireAck": true
          }
		}
	  ]
	}`
	s.Run("create rule error when plugin not installed", func() {
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusBadRequest, resp.StatusCode)
	})
	s.Run("install plugin and check status", func() {
		// zip the plugin dir
		pysamDir := filepath.Join(PWD, "sdk", "python", "example", "pysam")
		pysamZipPath := "/tmp/pysam.zip"
		err := zipDirectory(pysamDir, pysamZipPath)
		s.Require().NoError(err)
		defer func() {
			os.Remove(pysamZipPath)
		}()
		// install the plugin
		resp, err := client.Post("plugins/portables", `{"name":"pysam","file":"file:///tmp/pysam.zip"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
	})
	s.Run("check plugin info", func() {
		// check the plugin status
		resp, err := client.Get("plugins/portables")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		payload, err := io.ReadAll(resp.Body)
		s.NoError(err)
		defer resp.Body.Close()
		pwd, err := os.Getwd()
		s.Require().NoError(err)
		exp := fmt.Sprintf("[{\"name\":\"pysam\",\"version\":\"v1.0.0\",\"language\":\"python\",\"executable\":\"%s\",\"sources\":[\"pyjson\"],\"sinks\":[\"print\"],\"functions\":[\"revert\"]}]", filepath.Join(pwd, "..", "plugins", "portable", "pysam", "pysam.py"))
		s.Require().Equal(exp, string(payload))
	})
	s.Run("test rule with plugin", func() {
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Equal(http.StatusBadRequest, resp.StatusCode)

		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.T().Log(GetResponseText(resp))
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		// Check rule status after a while
		ticker := time.NewTicker(ConstantInterval)
		defer ticker.Stop()
		count := 20
		for count > 0 {
			<-ticker.C
			count--
			metrics, err := client.GetRuleStatus("rulePort1")
			s.Require().NoError(err)
			if metrics["sink_print_0_0_records_out_total"].(float64) > 5 {
				break
			}
		}
		metrics, err := client.GetRuleStatus("rulePort1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		sinkOut, ok := metrics["sink_print_0_0_records_out_total"]
		s.True(ok)
		s.T().Log(metrics)
		s.True(sinkOut.(float64) > 5)
		s.Equal("", metrics["source_pyjsonStream_0_last_exception"])
	})
	s.Run("check plugin status", func() {
		// check the plugin status
		resp, err := client.Get("plugins/portables/pysam/status")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		payload, err := io.ReadAll(resp.Body)
		s.NoError(err)
		defer resp.Body.Close()
		s.T().Log(string(payload))
		s.Require().True(strings.Contains(string(payload), "{\"refCount\":{\"rulePort1\":2},\"status\":\"running\",\"errMsg\":\"\""))
	})
	s.Run("update plugin and check status", func() {
		// zip the plugin dir
		pysamDir := filepath.Join(PWD, "sdk", "python", "example", "pysam")
		pysamZipPath := "/tmp/pysam.zip"
		err := zipDirectory(pysamDir, pysamZipPath)
		s.Require().NoError(err)
		defer func() {
			os.Remove(pysamZipPath)
		}()
		// update the plugin
		resp, err := client.Req("plugins/portables/pysam", http.MethodPut, `{"name":"pysam","file":"file:///tmp/pysam.zip"}`)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	s.Run("check plugin status after update", func() {
		// check the plugin status
		resp, err := client.Get("plugins/portables/pysam/status")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		payload, err := io.ReadAll(resp.Body)
		s.NoError(err)
		defer resp.Body.Close()
		s.T().Log(string(payload))
		s.Require().True(strings.Contains(string(payload), "{\"refCount\":{\"rulePort1\":2},\"status\":\"running\",\"errMsg\":\"\""))
	})
	s.Run("test rule restart", func() {
		resp, err := client.RestartRule("rulePort1")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		// Check rule status after a while
		ticker := time.NewTicker(ConstantInterval)
		defer ticker.Stop()
		count := 20
		for count > 0 {
			<-ticker.C
			count--
			metrics, err := client.GetRuleStatus("rulePort1")
			s.Require().NoError(err)
			if metrics["sink_print_0_0_records_out_total"].(float64) > 5 {
				break
			}
		}
		metrics, err := client.GetRuleStatus("rulePort1")
		s.Require().NoError(err)
		s.Equal("running", metrics["status"])
		s.T().Log(metrics)
		sinkOut, ok := metrics["sink_print_0_0_records_out_total"]
		s.True(ok)
		s.T().Log(metrics)
		s.True(sinkOut.(float64) > 5)
		s.Equal("", metrics["source_pyjsonStream_0_last_exception"])
	})
	s.Run("clean up", func() {
		resp, err := client.DeleteRule("rulePort1")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)

		resp, err = client.DeleteStream("pyjsonStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete plugin", func() {
		resp, err := client.Delete("plugins/portables/pysam")
		s.NoError(err)
		s.Equal(http.StatusOK, resp.StatusCode)
	})
}

// zipDirectory zips the contents of the specified source directory into the target zip file
func zipDirectory(source string, target string) error {
	// Create the zip file
	zipFile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	// Create a new zip writer
	writer := zip.NewWriter(zipFile)
	defer writer.Close()

	// Walk through the source directory and add files to the zip
	return filepath.Walk(source, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the source directory itself
		if file == source {
			return nil
		}

		// Create a header for the file
		header, err := zip.FileInfoHeader(fi)
		if err != nil {
			return err
		}

		// Set the header name to the relative path
		relPath, err := filepath.Rel(source, file)
		if err != nil {
			return err
		}
		header.Name = relPath

		// If it's a directory, append a "/" to the name
		if fi.IsDir() {
			header.Name += "/"
		}

		// Create the writer for the file header
		writer, err := writer.CreateHeader(header)
		if err != nil {
			return err
		}

		// If it's a file, write its content to the zip
		if !fi.IsDir() {
			fileReader, err := os.Open(file)
			if err != nil {
				return err
			}
			defer fileReader.Close()

			// Copy the file content to the zip writer
			_, err = io.Copy(writer, fileReader)
			if err != nil {
				return err
			}
		}

		return nil
	})
}
