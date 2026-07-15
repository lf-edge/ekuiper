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

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/validate"
)

func init() {
	components["schema"] = schemaComp{}
}

type schemaComp struct{}

func (sc schemaComp) register() {
	err := schema.InitRegistry()
	if err != nil {
		panic(err)
	}
}

func (sc schemaComp) rest(r *mux.Router) {
	r.HandleFunc("/schemas/{type}", schemasHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/schemas/{type}/{name}/upload", schemaUploadHandler).Methods(http.MethodPut)
	r.HandleFunc("/schemas/{type}/{name}", schemaHandler).Methods(http.MethodPut, http.MethodDelete, http.MethodGet)
}

const maxSchemaUploadFieldSize = 4 << 10

type schemaUpload struct {
	path    string
	version string
}

func (u *schemaUpload) cleanup() {
	if u != nil && u.path != "" {
		_ = os.Remove(u.path)
	}
}

func (u *schemaUpload) fileURL() string {
	return (&url.URL{Scheme: "file", Path: u.path}).String()
}

type schemaUploadResponse struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

func schemaUploadHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	st := vars["type"]
	name := vars["name"]
	if err := validate.ValidateID(name); err != nil {
		handleErrorWithStatus(w, err, "", http.StatusBadRequest, logger)
		return
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" {
		if err == nil {
			err = fmt.Errorf("content type must be multipart/form-data")
		}
		handleErrorWithStatus(w, err, "Invalid content type", http.StatusUnsupportedMediaType, logger)
		return
	}
	mr, err := r.MultipartReader()
	if err != nil {
		handleErrorWithStatus(w, err, "Invalid multipart body", http.StatusBadRequest, logger)
		return
	}
	upload, err := receiveSchemaUpload(mr)
	if err != nil {
		handleErrorWithStatus(w, err, "Invalid multipart body", http.StatusBadRequest, logger)
		return
	}
	defer upload.cleanup()

	sch := &schema.Info{
		Type:     st,
		Name:     name,
		FilePath: upload.fileURL(),
		Version:  upload.version,
	}
	if err = sch.Validate(); err != nil {
		handleErrorWithStatus(w, err, "Invalid schema", http.StatusBadRequest, logger)
		return
	}
	created, err := schema.Upsert(sch)
	if err != nil {
		handleErrorWithStatus(w, err, "schema upsert error", http.StatusBadRequest, logger)
		return
	}

	payload, err := json.Marshal(schemaUploadResponse{Type: st, Name: name})
	if err != nil {
		handleErrorWithStatus(w, err, "Error encoding response", http.StatusInternalServerError, logger)
		return
	}
	w.Header().Set(ContentType, ContentTypeJSON)
	w.Header().Set("Location", fmt.Sprintf("/schemas/%s/%s", st, name))
	if created {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if _, err = w.Write(payload); err != nil {
		logger.Errorf("Error writing schema upload response: %v", err)
	}
}

func receiveSchemaUpload(mr *multipart.Reader) (_ *schemaUpload, retErr error) {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		return nil, err
	}
	tempDir := filepath.Join(dataDir, "uploads", "schemas")
	if err = os.MkdirAll(tempDir, 0o755); err != nil {
		return nil, err
	}
	upload := &schemaUpload{}
	defer func() {
		if retErr != nil {
			upload.cleanup()
		}
	}()
	fileSeen := false
	versionSeen := false
	for {
		part, nextErr := mr.NextPart()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			return nil, nextErr
		}
		fieldName := part.FormName()
		switch fieldName {
		case "file":
			if fileSeen {
				_ = part.Close()
				return nil, fmt.Errorf("file field must appear exactly once")
			}
			fileSeen = true
			ext := filepath.Ext(part.FileName())
			tempFile, createErr := os.CreateTemp(tempDir, ".upload-*"+ext)
			if createErr != nil {
				_ = part.Close()
				return nil, createErr
			}
			upload.path = tempFile.Name()
			_, copyErr := io.Copy(tempFile, part)
			closeErr := tempFile.Close()
			partCloseErr := part.Close()
			if copyErr != nil {
				return nil, copyErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			if partCloseErr != nil {
				return nil, partCloseErr
			}
		case "version":
			if versionSeen {
				_ = part.Close()
				return nil, fmt.Errorf("version field must not be repeated")
			}
			versionSeen = true
			value, readErr := io.ReadAll(io.LimitReader(part, maxSchemaUploadFieldSize+1))
			partCloseErr := part.Close()
			if readErr != nil {
				return nil, readErr
			}
			if partCloseErr != nil {
				return nil, partCloseErr
			}
			if len(value) > maxSchemaUploadFieldSize {
				return nil, fmt.Errorf("version field is too large")
			}
			upload.version = string(value)
		default:
			_, copyErr := io.Copy(io.Discard, part)
			partCloseErr := part.Close()
			if copyErr != nil {
				return nil, copyErr
			}
			if partCloseErr != nil {
				return nil, partCloseErr
			}
		}
	}
	if !fileSeen {
		return nil, fmt.Errorf("file field is required")
	}
	return upload, nil
}

func (sc schemaComp) exporter() ConfManager {
	return schemaExporter{}
}

func schemasHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	st := vars["type"]
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		l, err := schema.GetAllForType(st)
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		jsonResponse(l, w, logger)
	case http.MethodPost:
		sch := &schema.Info{Type: st}
		err := json.NewDecoder(r.Body).Decode(sch)
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding schema json", logger)
			return
		}
		if err = sch.Validate(); err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		if err = validate.ValidateID(sch.Name); err != nil {
			handleError(w, err, "", logger)
			return
		}
		err = schema.Register(sch)
		if err != nil {
			handleError(w, err, "schema create command error", logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		tmpl := template.Must(template.New("response").Parse("{{.Type}} schema {{.Name}} is created"))
		tmpl.Execute(w, sch)
	}
}

func schemaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	st := vars["type"]
	name := vars["name"]
	if err := validate.ValidateID(name); err != nil {
		handleError(w, err, "", logger)
		return
	}
	switch r.Method {
	case http.MethodGet:
		j, err := schema.GetSchema(st, name)
		if err != nil {
			handleError(w, err, "", logger)
			return
		} else if j == nil {
			handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), "", logger)
			return
		}
		jsonResponse(j, w, logger)
	case http.MethodDelete:
		err := schema.DeleteSchema(st, name)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete %s schema %s error", st, name), logger)
			return
		}
		sch := &schema.Info{Type: st, Name: name}
		tmpl := template.Must(template.New("response").Parse("{{.Type}} schema {{.Name}} is deleted"))
		err = tmpl.Execute(w, sch)
		if err != nil {
			handleError(w, err, "schema update command error", logger)
			return
		}
	case http.MethodPut:
		sch := &schema.Info{Type: st, Name: name}
		err := json.NewDecoder(r.Body).Decode(sch)
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding schema json", logger)
			return
		}
		if sch.Type != st || sch.Name != name {
			handleError(w, nil, "Invalid body: Type or name does not match", logger)
			return
		}
		if err = sch.Validate(); err != nil {
			handleError(w, nil, "Invalid body", logger)
			return
		}
		err = schema.CreateOrUpdateSchema(sch)
		if err != nil {
			handleError(w, err, "schema update command error", logger)
			return
		}
		tmpl := template.Must(template.New("response").Parse("{{.Type}} schema {{.Name}} is updated"))
		err = tmpl.Execute(w, sch)
		if err != nil {
			handleError(w, err, "schema update command error", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

type schemaExporter struct{}

func (e schemaExporter) Import(ctx context.Context, s map[string]string) map[string]string {
	return schema.ImportSchema(ctx, s)
}

func (e schemaExporter) PartialImport(ctx context.Context, s map[string]string) map[string]string {
	return schema.SchemaPartialImport(ctx, s)
}

func (e schemaExporter) Export() map[string]string {
	return schema.GetAllSchema()
}

func (e schemaExporter) Status() map[string]string {
	return schema.GetAllSchemaStatus()
}

func (e schemaExporter) Reset() {
	schema.UninstallAllSchema()
}

func (e schemaExporter) InstallScript(s string) (string, string) {
	return schema.GetSchemaInstallScript(s)
}
