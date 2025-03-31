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

package server

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/stretchr/testify/require"
)

func (suite *RestTestSuite) TestUploadHandler() {
	body := fmt.Sprintf(`{"name": "%v.json","content": "hello"}`, time.Now().Unix())
	req, _ := http.NewRequest(http.MethodPost, "/config/uploads", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusCreated, w.Code)
	body = fmt.Sprintf(`{"name": "../../../%v.json","content": "hello"}`, time.Now().Unix())
	req, _ = http.NewRequest(http.MethodPost, "/config/uploads", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusBadRequest, w.Code)
}
