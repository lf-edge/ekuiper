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

package main

import (
	context2 "context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/plugin/portable"
	"github.com/lf-edge/ekuiper/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"net/http"
	"sync"
	"time"
)

// Only support to test a single plugin Testing process.
// 0. Edit the testingPlugin variable to match your plugin meta.
// 1. Start this server, and wait for handshake.
// 2. Start or debug your plugin. Make sure the handshake completed.
// 3. Issue startSymbol/stopSymbol REST API to debug your plugin symbol.

// EDIT HERE: Define the plugins that you want to test.
var testingPlugin = &portable.PluginInfo{
	PluginMeta: runtime.PluginMeta{
		Name:       "mirror",
		Version:    "v1",
		Language:   "go",
		Executable: "mirror.py",
	},
	Sources:   []string{"pyjson"},
	Sinks:     []string{"print"},
	Functions: []string{"revert"},
}

var mockSinkData = []map[string]interface{}{
	{
		"name":  "hello",
		"count": 5,
	}, {
		"name":  "world",
		"count": 10,
	},
}

var mockFuncData = [][]interface{}{
	{"twelve"},
	{"eleven"},
}

var (
	ins     *runtime.PluginIns
	m       *portable.Manager
	ctx     api.StreamContext
	cancels sync.Map
)

func main() {
	var err error
	m, err = portable.MockManager(map[string]*portable.PluginInfo{testingPlugin.Name: testingPlugin})
	if err != nil {
		panic(err)
	}
	ins, err := startPluginIns(testingPlugin)
	if err != nil {
		panic(err)
	}
	defer ins.Stop()
	runtime.GetPluginInsManager().AddPluginIns(testingPlugin.Name, ins)
	c := context.WithValue(context.Background(), context.LoggerKey, conf.Log)
	ctx = c.WithMeta("rule1", "op1", &state.MemoryStore{}).WithInstance(1)
	server := createRestServer("127.0.0.1", 33333)
	server.ListenAndServe()
}

func startPluginIns(info *portable.PluginInfo) (*runtime.PluginIns, error) {
	conf.Log.Infof("create control channel")
	ctrlChan, err := runtime.CreateControlChannel(info.Name)
	if err != nil {
		return nil, fmt.Errorf("can't create new control channel: %s", err.Error())
	}
	conf.Log.Println("waiting handshake")
	err = ctrlChan.Handshake()
	if err != nil {
		return nil, fmt.Errorf("plugin %s control handshake error: %v", info.Name, err)
	}
	conf.Log.Println("plugin start running")
	return runtime.NewPluginInsForTest(info.Name, ctrlChan), nil
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
	switch ctrl.PluginType {
	case runtime.TYPE_SOURCE:
		source, err := m.Source(ctrl.SymbolName)
		if err != nil {
			http.Error(w, fmt.Sprintf("running source %s %v", ctrl.SymbolName, err), http.StatusBadRequest)
			return
		}
		newctx, cancel := ctx.WithCancel()
		if _, ok := cancels.LoadOrStore(ctrl.PluginType+ctrl.SymbolName, cancel); ok {
			http.Error(w, fmt.Sprintf("source symbol %s already exists", ctrl.SymbolName), http.StatusBadRequest)
			return
		}
		consumer := make(chan api.SourceTuple)
		errCh := make(chan error)
		go func() {
			defer func() {
				source.Close(newctx)
				cancels.Delete(ctrl.PluginType + ctrl.SymbolName)
			}()
			for {
				select {
				case tuple := <-consumer:
					fmt.Println(tuple)
				case err := <-errCh:
					conf.Log.Error(err)
					return
				case <-newctx.Done():
					return
				}
			}
		}()
		source.Configure("", ctrl.Config)
		go source.Open(newctx, consumer, errCh)
	case runtime.TYPE_SINK:
		sink, err := m.Sink(ctrl.SymbolName)
		if err != nil {
			http.Error(w, fmt.Sprintf("running sink %s %v", ctrl.SymbolName, err), http.StatusBadRequest)
			return
		}
		newctx, cancel := ctx.WithCancel()
		if _, ok := cancels.LoadOrStore(ctrl.PluginType+ctrl.SymbolName, cancel); ok {
			http.Error(w, fmt.Sprintf("source symbol %s already exists", ctrl.SymbolName), http.StatusBadRequest)
			return
		}
		sink.Configure(ctrl.Config)
		err = sink.Open(newctx)
		if err != nil {
			http.Error(w, fmt.Sprintf("open sink %s %v", ctrl.SymbolName, err), http.StatusBadRequest)
			return
		}
		go func() {
			defer func() {
				sink.Close(newctx)
				cancels.Delete(ctrl.PluginType + ctrl.SymbolName)
			}()
			for {
				for _, m := range mockSinkData {
					err = sink.Collect(newctx, m)
					if err != nil {
						fmt.Printf("cannot collect data: %v\n", err)
						continue
					}
					select {
					case <-ctx.Done():
						ctx.GetLogger().Info("stop sink")
						return
					default:
					}
					time.Sleep(1 * time.Second)
				}
			}
		}()
	case runtime.TYPE_FUNC:
		f, err := m.Function(ctrl.SymbolName)
		if err != nil {
			http.Error(w, fmt.Sprintf("running function %s %v", ctrl.SymbolName, err), http.StatusBadRequest)
			return
		}
		newctx, cancel := ctx.WithCancel()
		fc := context.NewDefaultFuncContext(newctx, 1)
		if _, ok := cancels.LoadOrStore(ctrl.PluginType+ctrl.SymbolName, cancel); ok {
			http.Error(w, fmt.Sprintf("source symbol %s already exists", ctrl.SymbolName), http.StatusBadRequest)
			return
		}
		go func() {
			defer func() {
				cancels.Delete(ctrl.PluginType + ctrl.SymbolName)
			}()
			for {
				for _, m := range mockFuncData {
					r, ok := f.Exec(m, fc)
					if !ok {
						fmt.Print("cannot exec func\n")
						continue
					}
					fmt.Println(r)
					select {
					case <-ctx.Done():
						ctx.GetLogger().Info("stop sink")
						return
					default:
					}
					time.Sleep(1 * time.Second)
				}
			}
		}()
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func stopSymbolHandler(w http.ResponseWriter, r *http.Request) {
	ctrl, err := decode(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid body: decode error %v", err), http.StatusBadRequest)
		return
	}
	if cancel, ok := cancels.Load(ctrl.PluginType + ctrl.SymbolName); ok {
		cancel.(context2.CancelFunc)()
		cancels.Delete(ctrl.PluginType + ctrl.SymbolName)
	} else {
		http.Error(w, fmt.Sprintf("Symbol %s already close", ctrl.SymbolName), http.StatusBadRequest)
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
