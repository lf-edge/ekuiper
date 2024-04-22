// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

//go:build service || !core

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/service"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

var serviceManager *service.Manager

func init() {
	components["service"] = serviceComp{}
}

type serviceComp struct{}

func (s serviceComp) register() {
	var err error
	serviceManager, err = service.InitManager()
	if err != nil {
		panic(err)
	}
	entries = append(entries, binder.FactoryEntry{Name: "external service", Factory: serviceManager, Weight: 1})
}

func (s serviceComp) rest(r *mux.Router) {
	r.HandleFunc("/services", servicesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/services/functions", serviceFunctionsHandler).Methods(http.MethodGet)
	r.HandleFunc("/services/functions/{name}", serviceFunctionHandler).Methods(http.MethodGet)
	r.HandleFunc("/services/{name}", serviceHandler).Methods(http.MethodDelete, http.MethodGet, http.MethodPut)
}

func (s serviceComp) exporter() ConfManager {
	return serviceExporter{}
}

func servicesHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		content, err := serviceManager.List()
		if err != nil {
			handleError(w, err, "service list command error", logger)
			return
		}
		jsonResponse(content, w, logger)
	case http.MethodPost:
		sd := &service.ServiceCreationRequest{}
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding the %s service request payload", logger)
			return
		}
		err = serviceManager.Create(sd)
		if err != nil {
			handleError(w, err, "service create command error", logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "service %s is created", sd.Name)
	}
}

func serviceHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]
	switch r.Method {
	case http.MethodDelete:
		err := serviceManager.Delete(name)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete service %s error", name), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "service %s is deleted", name)
	case http.MethodGet:
		j, err := serviceManager.Get(name)
		if err != nil {
			handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("describe service %s error", name), logger)
			return
		}
		jsonResponse(j, w, logger)
	case http.MethodPut:
		sd := &service.ServiceCreationRequest{}
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding the %s service request payload", logger)
			return
		}
		sd.Name = name
		err = serviceManager.Update(sd)
		if err != nil {
			handleError(w, err, "service update command error", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "service %s is updated", sd.Name)
	}
}

func serviceFunctionsHandler(w http.ResponseWriter, r *http.Request) {
	content, err := serviceManager.ListFunctions()
	if err != nil {
		handleError(w, err, "service list command error", logger)
		return
	}
	jsonResponse(content, w, logger)
}

func serviceFunctionHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	j, err := serviceManager.GetFunction(name)
	if err != nil {
		handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("describe function %s error", name), logger)
		return
	}
	jsonResponse(j, w, logger)
}

type serviceExporter struct{}

func (e serviceExporter) Import(ctx context.Context, services map[string]string) map[string]string {
	return serviceManager.ImportServices(ctx, services)
}

func (e serviceExporter) PartialImport(ctx context.Context, services map[string]string) map[string]string {
	return serviceManager.ImportPartialServices(ctx, services)
}

func (e serviceExporter) Export() map[string]string {
	return serviceManager.GetAllServices()
}

func (e serviceExporter) Status() map[string]string {
	return serviceManager.GetAllServicesStatus()
}

func (e serviceExporter) Reset() {
	serviceManager.UninstallAllServices()
}
