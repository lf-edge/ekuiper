package httptestx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"

	"github.com/gorilla/mux"
)

// JSONOut using to encode payload as json format.
func JSONOut(w http.ResponseWriter, out interface{}) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	err := enc.Encode(out)
	// Problems encoding
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

const (
	DefaultToken = "privatisation"
	RefreshToken = "privaterefresh"
)

func tokenEndpointHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		body := &struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}{}
		err := json.NewDecoder(r.Body).Decode(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		if body.Username != "admin" || body.Password != "0000" {
			http.Error(w, "invalid username or password", http.StatusBadRequest)
		}
		out := &struct {
			Token        string `json:"token"`
			RefreshToken string `json:"refresh_token"`
			ClientId     string `json:"client_id"`
			Expires      int64  `json:"expires"`
		}{
			Token:        DefaultToken,
			RefreshToken: RefreshToken,
			ClientId:     "test",
			Expires:      36000,
		}
		JSONOut(w, out)
	}
}

func refreshTokenEndpointHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		if token != "Bearer "+DefaultToken {
			http.Error(w, "invalid token", http.StatusBadRequest)
		}
		rt := r.Header.Get("RefreshToken")
		if rt != RefreshToken {
			http.Error(w, "invalid refresh token", http.StatusBadRequest)
		}
		out := &struct {
			Token        string `json:"token"`
			RefreshToken string `json:"refresh_token"`
			ClientId     string `json:"client_id"`
			Expires      int64  `json:"expires"`
		}{
			Token:        DefaultToken,
			RefreshToken: RefreshToken,
			ClientId:     "test",
			Expires:      36000,
		}
		JSONOut(w, out)
	}
}

// MockServerRouterOption used to customized http routers,
// you can use this type to register your handlers on mock serer.
//
// The second argument called ctx is server provided global state key-value cache,
// you can use this cache to store customized variables in multiple requests.
type MockServerRouterOption func(r *mux.Router, ctx *sync.Map) error

// ResponseSnapshot is snapshots for handler response.
type ResponseSnapshot struct {
	Body    *bytes.Buffer
	Code    int
	Headers http.Header
	Method  string
}

// ResponseSnapshotMiddleware will snapshotting handlers response and append to give
// snapshots slice.
func ResponseSnapshotMiddleware(snapshots *[]*ResponseSnapshot) mux.MiddlewareFunc {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, r)

			copiedBodyBuf := bytes.NewBuffer([]byte{})
			_, err := io.Copy(copiedBodyBuf, rec.Body)
			if err != nil {
				panic(fmt.Sprintf("copy body buffer has error: %s", err))
			}
			// snapshotting
			snapshot := new(ResponseSnapshot)
			snapshot.Body = copiedBodyBuf
			snapshot.Code = rec.Code
			snapshot.Headers = make(http.Header, 0)
			snapshot.Method = r.Method
			for k, vs := range rec.Result().Header {
				for _, v := range vs {
					snapshot.Headers.Add(k, v)
				}
			}
			*snapshots = append(*snapshots, snapshot)

			// write everything to actual response writer
			for k, v := range rec.Result().Header {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.Code)
			rec.Body.WriteTo(w)
		})
	}
}

// WithBuiltinTestDataEndpoints creates simple data endpoints and register
// to router.
func WithBuiltinTestDataEndpoints() MockServerRouterOption {
	return func(r *mux.Router, ctx *sync.Map) error {
		key := "random_device_idx"
		ctx.Store(key, 0)

		// simple data endpoint
		r.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
			token := r.Header.Get("Authorization")
			if token != "Bearer "+DefaultToken {
				http.Error(w, "invalid token", http.StatusBadRequest)
			}
			out := &struct {
				DeviceId    string  `json:"device_id"`
				Temperature float64 `json:"temperature"`
				Humidity    float64 `json:"humidity"`
			}{
				DeviceId:    "device1",
				Temperature: 25.5,
				Humidity:    60.0,
			}
			JSONOut(w, out)
		}).Methods(http.MethodGet)

		// Return same data for 3 times
		r.HandleFunc("/data2", func(w http.ResponseWriter, r *http.Request) {
			var i int
			v, _ := ctx.Load(key)
			i = v.(int)
			out := &struct {
				Code int `json:"code"`
				Data struct {
					DeviceId    string  `json:"device_id"`
					Temperature float64 `json:"temperature"`
					Humidity    float64 `json:"humidity"`
				} `json:"data"`
			}{
				Code: 200,
				Data: struct {
					DeviceId    string  `json:"device_id"`
					Temperature float64 `json:"temperature"`
					Humidity    float64 `json:"humidity"`
				}{
					DeviceId:    "device" + strconv.Itoa(i/3),
					Temperature: 25.5,
					Humidity:    60.0,
				},
			}
			i++
			ctx.Store(key, i)
			JSONOut(w, out)
		}).Methods(http.MethodGet)

		// data3 returns json array
		r.HandleFunc("/data3", func(w http.ResponseWriter, r *http.Request) {
			out := []*struct {
				Code int `json:"code"`
				Data struct {
					DeviceId    string  `json:"device_id"`
					Temperature float64 `json:"temperature"`
					Humidity    float64 `json:"humidity"`
				} `json:"data"`
			}{
				{
					Code: 200,
					Data: struct {
						DeviceId    string  `json:"device_id"`
						Temperature float64 `json:"temperature"`
						Humidity    float64 `json:"humidity"`
					}{
						DeviceId:    "d1",
						Temperature: 25.5,
						Humidity:    60.0,
					},
				},
				{
					Code: 200,
					Data: struct {
						DeviceId    string  `json:"device_id"`
						Temperature float64 `json:"temperature"`
						Humidity    float64 `json:"humidity"`
					}{
						DeviceId:    "d2",
						Temperature: 25.5,
						Humidity:    60.0,
					},
				},
			}
			JSONOut(w, out)
		}).Methods(http.MethodGet)

		// data4 receives time range in url
		r.HandleFunc("/data4", func(w http.ResponseWriter, r *http.Request) {
			device := r.URL.Query().Get("device")
			s := r.URL.Query().Get("start")
			e := r.URL.Query().Get("end")

			start, _ := strconv.ParseInt(s, 10, 64)
			end, _ := strconv.ParseInt(e, 10, 64)

			out := &struct {
				Code int `json:"code"`
				Data struct {
					DeviceId    string `json:"device_id"`
					Temperature int64  `json:"temperature"`
					Humidity    int64  `json:"humidity"`
				} `json:"data"`
			}{
				Code: 200,
				Data: struct {
					DeviceId    string `json:"device_id"`
					Temperature int64  `json:"temperature"`
					Humidity    int64  `json:"humidity"`
				}{
					DeviceId:    device,
					Temperature: start % 50,
					Humidity:    end % 100,
				},
			}
			JSONOut(w, out)
		}).Methods(http.MethodGet)

		// data5 receives time range in body
		r.HandleFunc("/data5", func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Failed to read request body", http.StatusBadRequest)
				return
			}

			// Create a Person struct to hold the JSON data
			var ddd struct {
				Device string `json:"device"`
				Start  int64  `json:"start"`
				End    int64  `json:"end"`
			}

			// Unmarshal the JSON data into the Person struct
			err = json.Unmarshal(body, &ddd)
			if err != nil {
				http.Error(w, "Failed to parse JSON", http.StatusBadRequest)
				return
			}

			out := &struct {
				Code int `json:"code"`
				Data struct {
					DeviceId    string `json:"device_id"`
					Temperature int64  `json:"temperature"`
					Humidity    int64  `json:"humidity"`
				} `json:"data"`
			}{
				Code: 200,
				Data: struct {
					DeviceId    string `json:"device_id"`
					Temperature int64  `json:"temperature"`
					Humidity    int64  `json:"humidity"`
				}{
					DeviceId:    ddd.Device,
					Temperature: ddd.Start % 50,
					Humidity:    ddd.End % 100,
				},
			}
			JSONOut(w, out)
		}).Methods(http.MethodPost)

		r.HandleFunc("/data6", func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Failed to read request body", http.StatusBadRequest)
				return
			}

			// Create a Person struct to hold the JSON data
			var ddd struct {
				Device string `json:"device"`
				Token  string `json:"token"`
			}

			// Unmarshal the JSON data into the Person struct
			err = json.Unmarshal(body, &ddd)
			if err != nil {
				http.Error(w, "Failed to parse JSON", http.StatusBadRequest)
				return
			}

			if ddd.Token != DefaultToken {
				http.Error(w, "invalid token", http.StatusBadRequest)
			}

			out := &struct {
				DeviceId    string  `json:"device_id"`
				Temperature float64 `json:"temperature"`
				Humidity    float64 `json:"humidity"`
			}{
				DeviceId:    "device1",
				Temperature: 25.5,
				Humidity:    60.0,
			}
			JSONOut(w, out)
		}).Methods(http.MethodPost)

		return nil
	}
}

// MockAuthServer creates new test http server with listening port 52345.
func MockAuthServer(opts ...MockServerRouterOption) (server *httptest.Server, closer func()) {
	l, _ := net.Listen("tcp", "127.0.0.1:52345")
	router := mux.NewRouter()
	globalCache := new(sync.Map)

	// register authentication endpoints
	{
		router.HandleFunc("/token", tokenEndpointHandler()).Methods(http.MethodPost)          // [POST] /token
		router.HandleFunc("/refresh", refreshTokenEndpointHandler()).Methods(http.MethodPost) // [POST] /refresh
	}

	// apply options if not empty
	if len(opts) > 0 {
		for _, opt := range opts {
			if err := opt(router, globalCache); err != nil {
				panic(err)
			}
		}
	}

	server = httptest.NewUnstartedServer(router)
	err := server.Listener.Close()
	if err != nil {
		panic(err)
	}
	server.Listener = l
	return server, func() {
		globalCache.Range(func(key, value any) bool {
			globalCache.Delete(key)
			return true
		})
		server.Close()
	}
}
