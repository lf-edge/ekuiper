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

package middleware

import (
	"fmt"
	"net/http"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/jwt"
)

var notAuth = []string{"/", "/ping"}

var AuditRestLog = func(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conf.Log.Infoln("visit %v %v", r.Method, r.URL)
		next.ServeHTTP(w, r)
	})
}

var Auth = func(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath := r.URL.Path
		for _, value := range notAuth {
			if value == requestPath {
				next.ServeHTTP(w, r)
				return
			}
		}

		tokenHeader := r.Header.Get("Authorization")

		if tokenHeader == "" {
			http.Error(w, "missing_token", http.StatusUnauthorized)
			return
		}
		tk, err := jwt.ParseToken(tokenHeader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		hit := false
		for _, value := range tk.RegisteredClaims.Audience {
			if value == "eKuiper" {
				hit = true
				break
			}
		}
		if !hit {
			http.Error(w, fmt.Sprintf("audience field should contain eKuiper, but got %s", tk.RegisteredClaims.Audience), http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
