// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// Only support to test a single plugin Testing process.
// 0. Edit the testingPlugin variable to match your plugin meta.
// 1. Start this server, and wait for handshake.
// 2. Start or debug your plugin. Make sure the handshake completed.
// 3. Issue startSymbol/stopSymbol REST API to debug your plugin symbol.

// EDIT HERE: Define the plugins that you want to test.
var testingPlugin = &portable.PluginInfo{
	PluginMeta: runtime.PluginMeta{
		Name:       "pysam",
		Version:    "v1",
		Language:   "python",
		Executable: "pysam.py",
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
	if conf.Config == nil {
		conf.Config = &model.KuiperConf{}
	}
	conf.Config.Portable.InitTimeout = cast.DurationConf(5 * time.Minute)
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
		Addr: cast.JoinHostPortInt(ip, port),
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
		ss, err := m.Source(ctrl.SymbolName)
		if err != nil {
			http.Error(w, fmt.Sprintf("running source %s %v", ctrl.SymbolName, err), http.StatusBadRequest)
			return
		}
		source := ss.(*runtime.PortableSource)
		newctx, cancel := ctx.WithCancel()
		if _, ok := cancels.LoadOrStore(ctrl.PluginType+ctrl.SymbolName, cancel); ok {
			http.Error(w, fmt.Sprintf("source symbol %s already exists", ctrl.SymbolName), http.StatusBadRequest)
			return
		}
		go func() {
			<-newctx.Done()
			source.Close(newctx)
			cancels.Delete(ctrl.PluginType + ctrl.SymbolName)
		}()
		err = source.Provision(newctx, ctrl.Config)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = source.Connect(newctx, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		go source.Subscribe(newctx, func(_ api.StreamContext, data any, _ map[string]any, _ time.Time) {
			fmt.Printf("%v\n", data)
		}, func(ctx api.StreamContext, err error) {
			fmt.Println(err.Error())
		})
	case runtime.TYPE_SINK:
		ss, err := m.Sink(ctrl.SymbolName)
		if err != nil {
			http.Error(w, fmt.Sprintf("running sink %s %v", ctrl.SymbolName, err), http.StatusBadRequest)
			return
		}
		sink := ss.(*runtime.PortableSink)
		newctx, cancel := ctx.WithCancel()
		if _, ok := cancels.LoadOrStore(ctrl.PluginType+ctrl.SymbolName, cancel); ok {
			http.Error(w, fmt.Sprintf("source symbol %s already exists", ctrl.SymbolName), http.StatusBadRequest)
			return
		}
		err = sink.Provision(newctx, ctrl.Config)
		if err != nil {
			http.Error(w, fmt.Sprintf("open sink %s %v", ctrl.SymbolName, err), http.StatusBadRequest)
			return
		}
		err = sink.Connect(newctx, nil)
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
					jsonStr, err := json.Marshal(m)
					if err != nil {
						fmt.Printf("cannot collect data: %v\n", err)
						continue
					}
					err = sink.Collect(newctx, &xsql.RawTuple{Rawdata: jsonStr})
					if err != nil {
						fmt.Printf("cannot collect data: %v\n", err)
						continue
					}
					select {
					case <-newctx.Done():
						fmt.Println("stop sink")
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
					r, ok := f.Exec(fc, m)
					if !ok {
						fmt.Print("cannot exec func\n")
						continue
					}
					fmt.Println(r)
					select {
					case <-newctx.Done():
						fmt.Println("stop func")
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
