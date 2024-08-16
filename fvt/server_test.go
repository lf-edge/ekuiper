// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ServerTestSuite struct {
	suite.Suite
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}

func (s *ServerTestSuite) TestServerStop() {
	s.Run("ping rest service", func() {
		resp, err := client.Get("ping")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	s.Run("get http service status", func() {
		resp, err := client.Get("")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		s.Require().NoError(err)
		m := make(map[string]any)
		err = json.Unmarshal(body, &m)
		s.Require().NoError(err)
		s.Require().Equal("fvt", m["version"])
		s.NotNil(m["os"])
		s.NotNil(m["arch"])
		s.NotNil(m["upTimeSeconds"])
		s.NotNil(m["cpuUsage"])
		s.NotNil(m["memoryUsed"])
		s.NotNil(m["memoryTotal"])
	})
}
