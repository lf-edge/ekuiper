// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package source

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"net/http"
	"strings"
)

type HTTPPushConf struct {
	Server   string `json:"server"`
	Endpoint string `json:"endpoint"`
}

type HTTPPushSource struct {
	conf *HTTPPushConf
}

func (hps *HTTPPushSource) Configure(endpoint string, props map[string]interface{}) error {
	cfg := &HTTPPushConf{
		Server:   ":9999",
		Endpoint: "",
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return err
	}
	if strings.Trim(cfg.Server, " ") == "" {
		return fmt.Errorf("property `server` is required")
	}
	if !strings.HasPrefix(endpoint, "/") {
		return fmt.Errorf("property `endpoint` must start with /")
	}

	cfg.Endpoint = endpoint
	hps.conf = cfg
	conf.Log.Debugf("Initialized with configurations %#v.", cfg)
	return nil
}

func (hps *HTTPPushSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	r := mux.NewRouter()
	meta := make(map[string]interface{})
	r.HandleFunc(hps.conf.Endpoint, func(w http.ResponseWriter, r *http.Request) {
		ctx.GetLogger().Debugf("receive getGPS request")
		defer r.Body.Close()
		m := make(map[string]interface{})
		err := json.NewDecoder(r.Body).Decode(&m)
		if err != nil {
			handleError(w, err, "Fail to decode data")
			return
		}
		ctx.GetLogger().Debugf("message: %v", m)
		select {
		case consumer <- api.NewDefaultSourceTuple(m, meta):
			ctx.GetLogger().Debugf("send data from http push source")
		case <-ctx.Done():
			handleError(w, err, "stopped")
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	// TODO global server
	srv := &http.Server{
		Addr:    hps.conf.Server,
		Handler: r,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			ctx.GetLogger().Errorf("listen: %s", err)
			infra.DrainError(ctx, err, errCh)
		}
	}()
	ctx.GetLogger().Infof("http server source listen at: %s", hps.conf.Server)
	select {
	case <-ctx.Done():
		ctx.GetLogger().Infof("shutting down server...")
		if err := srv.Shutdown(ctx); err != nil {
			ctx.GetLogger().Errorf("shutdown: %s\n", err)
		}
		ctx.GetLogger().Infof("server exiting")
	}
}

func (hps *HTTPPushSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing HTTP push source")
	return nil
}

func handleError(w http.ResponseWriter, err error, prefix string) {
	message := prefix
	if message != "" {
		message += ": "
	}
	message += err.Error()
	http.Error(w, message, http.StatusBadRequest)
}
