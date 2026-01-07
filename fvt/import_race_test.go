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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// ImportRaceTestSuite tests race conditions in rule import
type ImportRaceTestSuite struct {
	suite.Suite
}

func TestImportRaceTestSuite(t *testing.T) {
	suite.Run(t, new(ImportRaceTestSuite))
}

func (s *ImportRaceTestSuite) SetupTest() {
	client.DeleteRule("raceTest")
	client.DeleteStream("raceTestStream")
}

func (s *ImportRaceTestSuite) TearDownTest() {
	client.DeleteRule("raceTest")
	client.DeleteStream("raceTestStream")
}

// TestConcurrentImportNewRule tests concurrent imports for a NEW rule (not in registry)
func (s *ImportRaceTestSuite) TestConcurrentImportNewRule() {
	s.Run("concurrent import new rule", func() {
		streamSql := `{"sql":"CREATE STREAM raceTestStream () WITH (DATASOURCE=\"test\", TYPE=\"mqtt\")"}`
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)

		importContent := `{
			"streams": {},
			"tables": {},
			"rules": {
				"raceTest": "{\"id\":\"raceTest\",\"sql\":\"SELECT * FROM raceTestStream\",\"actions\":[{\"log\":{}}]}"
			}
		}`

		concurrency := 5
		var wg sync.WaitGroup
		results := make([]int, concurrency)
		errors := make([]string, concurrency)

		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go func(idx int) {
				defer wg.Done()
				resp, err := client.Import(importContent)
				if err != nil {
					errors[idx] = err.Error()
					return
				}
				results[idx] = resp.StatusCode
				if resp.StatusCode != http.StatusOK {
					text, _ := GetResponseText(resp)
					errors[idx] = text
				}
			}(i)
		}
		wg.Wait()

		successCount := 0
		failureCount := 0
		for i, code := range results {
			if code == 200 {
				successCount++
			} else if errors[i] != "" && strings.Contains(errors[i], "already exist") {
				failureCount++
			}
		}

		s.T().Logf("Concurrent import NEW rule: %d successes, %d failures", successCount, failureCount)

		if failureCount > 0 {
			s.T().Logf("FOUND RACE: %d concurrent imports failed with 'already exists'", failureCount)
		}
	})
}

// TestConcurrentImportRunningRule tests concurrent imports on a RUNNING rule
func (s *ImportRaceTestSuite) TestConcurrentImportRunningRule() {
	s.Run("concurrent import on RUNNING rule (slow stop)", func() {
		streamSql := `{"sql":"CREATE STREAM raceTestStream () WITH (DATASOURCE=\"test\", TYPE=\"mqtt\")"}`
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)

		ruleSql := `{"id":"raceTest","sql":"SELECT * FROM raceTestStream","actions":[{"log":{}}]}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)

		time.Sleep(100 * time.Millisecond)

		resp, err = client.Get("rules/raceTest/status")
		s.Require().NoError(err)
		statusResult, _ := GetResponseResultMap(resp)
		s.T().Logf("Rule status: %v (should be running)", statusResult["status"])

		importContent := `{
			"streams": {},
			"tables": {},
			"rules": {
				"raceTest": "{\"id\":\"raceTest\",\"sql\":\"SELECT * FROM raceTestStream\",\"actions\":[{\"log\":{}}]}"
			}
		}`

		iterations := 10
		concurrency := 10
		totalSuccess := 0
		totalFailure := 0

		for iter := 0; iter < iterations; iter++ {
			var wg sync.WaitGroup
			results := make([]int, concurrency)
			errors := make([]string, concurrency)

			wg.Add(concurrency)
			for i := 0; i < concurrency; i++ {
				go func(idx int) {
					defer wg.Done()
					resp, err := client.Import(importContent)
					if err != nil {
						errors[idx] = err.Error()
						return
					}
					results[idx] = resp.StatusCode
					if resp.StatusCode != http.StatusOK {
						text, _ := GetResponseText(resp)
						errors[idx] = text
					}
				}(i)
			}
			wg.Wait()

			for i, code := range results {
				if code == 200 {
					totalSuccess++
				} else if errors[i] != "" && strings.Contains(errors[i], "already exist") {
					totalFailure++
				}
			}
		}

		s.T().Logf("Concurrent import RUNNING rule: %d successes, %d failures", totalSuccess, totalFailure)

		if totalFailure > 0 {
			s.T().Logf("FOUND RACE: %d concurrent updates on RUNNING rule failed", totalFailure)
		} else {
			s.T().Log("NO RACE: All concurrent imports on RUNNING rule succeeded")
		}
	})
}

// TestConcurrentImportDifferentVersions tests that higher version wins in concurrent imports
func (s *ImportRaceTestSuite) TestConcurrentImportDifferentVersions() {
	s.Run("higher version wins", func() {
		streamSql := `{"sql":"CREATE STREAM raceTestStream () WITH (DATASOURCE=\"test\", TYPE=\"mqtt\")"}`
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)

		versions := []string{"1.0.0", "2.0.0", "3.0.0", "1.5.0", "2.5.0"}
		var wg sync.WaitGroup
		results := make([]int, len(versions))
		errors := make([]string, len(versions))

		wg.Add(len(versions))
		for i, version := range versions {
			go func(idx int, ver string) {
				defer wg.Done()
				importContent := `{
					"streams": {},
					"tables": {},
					"rules": {
						"raceTest": "{\"id\":\"raceTest\",\"version\":\"` + ver + `\",\"sql\":\"SELECT * FROM raceTestStream\",\"actions\":[{\"log\":{}}]}"
					}
				}`
				resp, err := client.Import(importContent)
				if err != nil {
					errors[idx] = err.Error()
					return
				}
				results[idx] = resp.StatusCode
				if resp.StatusCode != http.StatusOK {
					text, _ := GetResponseText(resp)
					errors[idx] = text
				}
			}(i, version)
		}
		wg.Wait()

		for i, ver := range versions {
			if results[i] == 200 {
				s.T().Logf("Version %s: success", ver)
			} else {
				s.T().Logf("Version %s: failed - %s", ver, errors[i])
			}
		}

		resp, err = client.Get("rules/raceTest")
		s.Require().NoError(err)
		s.Require().Equal(200, resp.StatusCode)

		ruleResult, err := GetResponseResultMap(resp)
		s.Require().NoError(err)
		finalVersion := ruleResult["version"]
		s.T().Logf("Final rule version: %v", finalVersion)

		s.Require().Equal("3.0.0", finalVersion, "Highest version (3.0.0) should win")
	})
}

// TestConcurrentUpdateRule tests concurrent updates via PUT /rules/{id} (UpsertRule path)
func (s *ImportRaceTestSuite) TestConcurrentUpdateRule() {
	s.Run("concurrent update rule", func() {
		// 1. Create stream
		streamSql := `{"sql":"CREATE STREAM raceTestStream () WITH (DATASOURCE=\"test\", TYPE=\"mqtt\")"}`
		resp, err := client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)

		// 2. Create initial rule (Version 1.0.0)
		ruleSql := `{"id":"raceTest","sql":"SELECT * FROM raceTestStream","actions":[{"log":{}}],"version":"1.0.0"}`
		resp, err = client.CreateRule(ruleSql)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)

		// 3. Concurrent updates with diff versions
		versions := []string{"2.0.0", "3.0.0", "1.5.0", "2.5.0"}
		var wg sync.WaitGroup
		results := make([]int, len(versions))
		errors := make([]string, len(versions))

		wg.Add(len(versions))
		for i, version := range versions {
			go func(idx int, ver string) {
				defer wg.Done()
				// Update rule content
				updateContent := `{"id":"raceTest","sql":"SELECT * FROM raceTestStream","actions":[{"log":{}}],"version":"` + ver + `"}`
				resp, err := client.UpdateRule("raceTest", updateContent)
				if err != nil {
					errors[idx] = err.Error()
					return
				}
				results[idx] = resp.StatusCode
				if resp.StatusCode != http.StatusOK {
					text, _ := GetResponseText(resp)
					errors[idx] = text
				}
			}(i, version)
		}
		wg.Wait()

		// 4. Verification
		for i, ver := range versions {
			if results[i] == 200 {
				s.T().Logf("Update to %s: success", ver)
			} else {
				s.T().Logf("Update to %s: failed - %s", ver, errors[i])
			}
		}

		resp, err = client.Get("rules/raceTest")
		s.Require().NoError(err)
		s.Require().Equal(200, resp.StatusCode)

		ruleResult, err := GetResponseResultMap(resp)
		s.Require().NoError(err)
		finalVersion := ruleResult["version"]
		s.T().Logf("Final rule version in registry: %v", finalVersion)

		s.Require().Equal("3.0.0", finalVersion, "Highest version (3.0.0) should win and persist")
	})
}
