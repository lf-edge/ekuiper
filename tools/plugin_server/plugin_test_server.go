// Copyright 2021 EMQ Technologies Co., Ltd.
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

package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"net/http"
	"time"
)

func main() {
	var err error
	ins, err = startPluginIns()
	if err != nil {
		panic(err)
	}
	defer ins.Stop()
	c := context.WithValue(context.Background(), context.LoggerKey, conf.Log)
	ctx = c.WithMeta("rule1", "op1", &state.MemoryStore{}).WithInstance(1)
	server := createRestServer("127.0.0.1", 33333)
	server.ListenAndServe()
}

const (
	pluginName = "$$test"
)

var (
	ctx api.StreamContext
	ins *runtime.PluginIns
)

func startPluginIns() (*runtime.PluginIns, error) {
	conf.Log.Infof("create control channel")
	ctrlChan, err := runtime.CreateControlChannel(pluginName)
	if err != nil {
		return nil, fmt.Errorf("can't create new control channel: %s", err.Error())
	}
	conf.Log.Println("waiting handshake")
	err = ctrlChan.Handshake()
	if err != nil {
		return nil, fmt.Errorf("plugin %s control handshake error: %v", pluginName, err)
	}
	conf.Log.Println("plugin start running")
	return runtime.NewPluginIns(pluginName, ctrlChan, nil), nil
}

func createRestServer(ip string, port int) *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/symbol/start", startSymbolHandler).Methods(http.MethodPost)
	r.HandleFunc("/symbol/stop", stopSymbolHandler).Methods(http.MethodPost)
	server := &http.Server{
		Addr: fmt.Sprintf("%s:%d", ip, port),
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 60 * 5,
		ReadTimeout:  time.Second * 60 * 5,
		IdleTimeout:  time.Second * 60,
		Handler:      handlers.CORS(handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Type", "Content-Language", "Origin"}))(r),
	}
	server.SetKeepAlivesEnabled(false)
	return server
}

func startSymbolHandler(w http.ResponseWriter, r *http.Request) {
	ctrl, err := decode(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid body: decode error %v", err), http.StatusBadRequest)
		return
	}
	err = ins.StartSymbol(ctx, ctrl)
	if err != nil {
		http.Error(w, fmt.Sprintf("start symbol error: %v", err), http.StatusBadRequest)
		return
	}
	go receive(ctx)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func stopSymbolHandler(w http.ResponseWriter, r *http.Request) {
	ctrl, err := decode(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid body: decode error %v", err), http.StatusBadRequest)
		return
	}
	err = ins.StopSymbol(ctx, ctrl)
	if err != nil {
		http.Error(w, fmt.Sprintf("start symbol error: %v", err), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func decode(r *http.Request) (*runtime.Control, error) {
	defer r.Body.Close()
	ctrl := &runtime.Control{}
	err := json.NewDecoder(r.Body).Decode(ctrl)
	return ctrl, err
}

func receive(ctx api.StreamContext) {
	dataCh, err := runtime.CreateSourceChannel(ctx)
	if err != nil {
		fmt.Printf("cannot create source channel: %s\n", err.Error())
	}
	for {
		var msg []byte
		msg, err := dataCh.Recv()
		if err != nil {
			fmt.Printf("cannot receive from mangos Socket: %s\n", err.Error())
			return
		}
		result := &api.DefaultSourceTuple{}
		e := json.Unmarshal(msg, result)
		if e != nil {
			ctx.GetLogger().Errorf("Invalid data format, cannot decode %s to json format with error %s", string(msg), e)
			continue
		}
		fmt.Println(result)
		select {
		case <-ctx.Done():
			ctx.GetLogger().Info("stop source")
			return
		default:
		}
	}
}
