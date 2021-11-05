package middleware

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/jwt"
	"net/http"
)

var notAuth = []string{"/", "/ping"}

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
		if tk.StandardClaims.Audience != "eKuiper" {
			http.Error(w, fmt.Sprintf("audience field should be eKuiper, but got %s", tk.StandardClaims.Audience), http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
