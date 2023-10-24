// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/gorilla/mux"
	"google.golang.org/grpc"

	kconf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/topotest"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type RestHelloRequest struct {
	Name string `json:"name,omitempty"`
}

type RestHelloReply struct {
	Message string `json:"message,omitempty"`
}

type ObjectDetectRequest struct {
	Command string `json:"cmd,omitempty"`
	Image   string `json:"base64_img,omitempty"`
}

type ObjectDetectResponse struct {
	Info   string `json:"cmd,omitempty"`
	Code   int    `json:"base64_img,omitempty"`
	Image  string `json:"image,omitempty"`
	Result string `json:"result,omitempty"`
	Type   string `json:"type,omitempty"`
}

//type Box struct {
//	X int32 `json:"x,omitempty"`
//	Y int32 `json:"y,omitempty"`
//	W int32 `json:"w,omitempty"`
//	H int32 `json:"h,omitempty"`
//}

//type FeatureResult struct {
//	Features []float64 `json:"features,omitempty"`
//	Box      Box       `json:"box,omitempty"`
//}

type EncodedRequest struct {
	Name string `json:"name,omitempty"`
	Size int    `json:"size,omitempty"`
}

type ShelfMessage struct {
	Id    string `json:"id,omitempty"`
	Theme string `json:"theme,omitempty"`
}

type ShelfMessageOut struct {
	Id    int64  `json:"id,omitempty"`
	Theme string `json:"theme,omitempty"`
}

type BookMessage struct {
	Id     int64  `json:"id,omitempty"`
	Author string `json:"author,omitempty"`
	Title  string `json:"title,omitempty"`
}

type MessageMessage struct {
	Text string `json:"text,omitempty"`
}

type SchemalessShelfMessage struct {
	Id    int64  `json:"id,omitempty"`
	Theme string `json:"theme,omitempty"`
}

func TestRestService(t *testing.T) {
	// mock server, the port is set in the sample.json
	l, err := net.Listen("tcp", "127.0.0.1:51234")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	count := 0
	router := mux.NewRouter()
	router.HandleFunc("/SayHello", func(w http.ResponseWriter, r *http.Request) {
		body := &RestHelloRequest{}
		err := json.NewDecoder(r.Body).Decode(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		out := &RestHelloReply{Message: body.Name}
		jsonOut(w, out)
	}).Methods(http.MethodPost)
	router.HandleFunc("/object_detection", func(w http.ResponseWriter, r *http.Request) {
		req := &ObjectDetectRequest{}
		err := json.NewDecoder(r.Body).Decode(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		if req.Image == "" {
			http.Error(w, "image is not found", http.StatusBadRequest)
		}
		out := &ObjectDetectResponse{
			Info:   req.Command,
			Code:   200,
			Image:  req.Image,
			Result: req.Command + " success",
			Type:   "S",
		}
		jsonOut(w, out)
	}).Methods(http.MethodPost)
	router.HandleFunc("/getStatus", func(w http.ResponseWriter, r *http.Request) {
		result := count%2 == 0
		count++
		io.WriteString(w, fmt.Sprintf("%v", result))
	}).Methods(http.MethodPost)
	router.HandleFunc("/RestEncodedJson", func(w http.ResponseWriter, r *http.Request) {
		req := &EncodedRequest{}
		err := json.NewDecoder(r.Body).Decode(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		io.WriteString(w, req.Name)
	}).Methods(http.MethodPost)
	router.HandleFunc("/bookshelf/v1/shelves", func(w http.ResponseWriter, r *http.Request) {
		req := &ShelfMessage{}
		err := json.NewDecoder(r.Body).Decode(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		if req.Id == "" || req.Theme == "" {
			http.Error(w, "empty request", http.StatusBadRequest)
		}
		idint, _ := strconv.Atoi(req.Id)
		out := ShelfMessageOut{Id: int64(idint), Theme: req.Theme}
		jsonOut(w, out)
	}).Methods(http.MethodPost)
	router.HandleFunc("/bookshelf/v1/shelves/{shelf}/books/{book}", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		vars := mux.Vars(r)
		shelf, book := vars["shelf"], vars["book"]
		if shelf == "" || book == "" {
			http.Error(w, "empty request", http.StatusBadRequest)
		}
		idint, _ := strconv.Atoi(book)
		out := BookMessage{Id: int64(idint), Author: "NA", Title: "title_" + book}
		jsonOut(w, out)
	}).Methods(http.MethodGet)
	router.HandleFunc("/messaging/v1/messages/{name}", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		vars := mux.Vars(r)
		name := vars["name"]
		if name == "" {
			http.Error(w, "empty request", http.StatusBadRequest)
		}
		out := MessageMessage{Text: name + " content"}
		jsonOut(w, out)
	}).Methods(http.MethodGet)
	router.HandleFunc("/messaging/v1/messages/filter/{name}", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		vars := mux.Vars(r)
		name := vars["name"]
		q := r.URL.Query()
		rev, sub := q.Get("revision"), q.Get("sub.subfield")
		if name == "" || rev == "" || sub == "" {
			http.Error(w, "empty request", http.StatusBadRequest)
		}
		out := MessageMessage{Text: name + rev + sub}
		jsonOut(w, out)
	}).Methods(http.MethodGet)
	router.HandleFunc("/messaging/v1/messages/{name}", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		vars := mux.Vars(r)
		name := vars["name"]
		if name == "" {
			http.Error(w, "empty request", http.StatusBadRequest)
		}
		body := &MessageMessage{}
		err := json.NewDecoder(r.Body).Decode(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		out := MessageMessage{Text: body.Text}
		jsonOut(w, out)
	}).Methods(http.MethodPut, http.MethodPatch)
	server := httptest.NewUnstartedServer(router)
	server.Listener.Close()
	server.Listener = l

	// Start the server.
	server.Start()

	defer server.Close()
	// Reset
	streamList := []string{"helloStr", "commands", "fakeBin", "shelves", "demo", "mes", "optional_commands"}
	topotest.HandleStream(false, streamList, t)
	// Data setup
	tests := []topotest.RuleTest{
		{
			Name: `TestRestRule1`,
			Sql:  `SELECT helloFromRest(name) as wc FROM helloStr`,
			R: [][]map[string]interface{}{
				{{
					"wc": map[string]interface{}{
						"message": "world",
					},
				}},
				{{
					"wc": map[string]interface{}{
						"message": "golang",
					},
				}},
				{{
					"wc": map[string]interface{}{
						"message": "peacock",
					},
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule2`,
			Sql:  `SELECT objectDetectFromRest(cmd, base64_img)->result FROM commands`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": "get success",
				}},
				{{
					"kuiper_field_0": "detect success",
				}},
				{{
					"kuiper_field_0": "delete success",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule3`,
			Sql:  `SELECT objectDetectFromRest(*)->result FROM commands`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": "get success",
				}},
				{{
					"kuiper_field_0": "detect success",
				}},
				{{
					"kuiper_field_0": "delete success",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
			//}, {
			//	Name: `TestRestRule3`,
			//	Sql:  `SELECT getFeatureFromRest(self)->feature[0]->box->h FROM fakeBin`,
			//	R: [][]map[string]interface{}{
			//		{{
			//			"kuiper_field_0": 106,
			//		}},
			//		{{
			//			"kuiper_field_0": 107,
			//		}},
			//		{{
			//			"kuiper_field_0": 108,
			//		}},
			//	},
			//	M: map[string]interface{}{
			//		"op_2_project_0_exceptions_total":   int64(0),
			//		"op_2_project_0_process_latency_us": int64(0),
			//		"op_2_project_0_records_in_total":   int64(3),
			//		"op_2_project_0_records_out_total":  int64(3),
			//
			//		"sink_mockSink_0_exceptions_total":  int64(0),
			//		"sink_mockSink_0_records_in_total":  int64(3),
			//		"sink_mockSink_0_records_out_total": int64(3),
			//	},
		}, {
			Name: `TestRestRule4`,
			Sql:  `SELECT getStatusFromRest(), cmd FROM commands`,
			R: [][]map[string]interface{}{
				{{
					"getStatusFromRest": true,
					"cmd":               "get",
				}},
				{{
					"getStatusFromRest": false,
					"cmd":               "detect",
				}},
				{{
					"getStatusFromRest": true,
					"cmd":               "delete",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule5`,
			Sql:  `SELECT restEncodedJson(encoded_json) as name FROM commands`,
			R: [][]map[string]interface{}{
				{{
					"name": "name1",
				}},
				{{
					"name": "name2",
				}},
				{{
					"name": "name3",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule6`,
			Sql:  `SELECT CreateShelf(shelf)->theme as theme FROM shelves`,
			R: [][]map[string]interface{}{
				{{
					"theme": "tandra",
				}},
				{{
					"theme": "claro",
				}},
				{{
					"theme": "dark",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule7`,
			Sql:  `SELECT GetBook(size, ts)->title as title FROM demo WHERE size > 3 `,
			R: [][]map[string]interface{}{
				{{
					"title": "title_1541152486822",
				}},
				{{
					"title": "title_1541152488442",
				}},
			},
			M: map[string]interface{}{
				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),
			},
		}, {
			Name: `TestRestRule8`,
			Sql:  `SELECT GetMessage(concat("messages/",ts))->text as message FROM demo WHERE size > 3`,
			R: [][]map[string]interface{}{
				{{
					"message": "1541152486822 content",
				}},
				{{
					"message": "1541152488442 content",
				}},
			},
			M: map[string]interface{}{
				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(2),
				"sink_mockSink_0_records_out_total": int64(2),
			},
		}, {
			Name: `TestRestRule9`,
			Sql:  `SELECT SearchMessage(name, size, shelf)->text as message FROM shelves`,
			R: [][]map[string]interface{}{
				{{
					"message": "name12sub1",
				}},
				{{
					"message": "name23sub2",
				}},
				{{
					"message": "name34sub3",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
			// TODO support * as one of the parameters
			//},{
			//	Name: `TestRestRule10`,
			//	Sql:  `SELECT UpdateMessage(message_id, *)->text as message FROM mes`,
			//	R: [][]map[string]interface{}{
			//		{{
			//			"message": "message1",
			//		}},
			//		{{
			//			"message": "message2",
			//		}},
			//		{{
			//			"message": "message3",
			//		}},
			//	},
			//	M: map[string]interface{}{
			//		"op_2_project_0_exceptions_total":   int64(0),
			//		"op_2_project_0_process_latency_us": int64(0),
			//		"op_2_project_0_records_in_total":   int64(3),
			//		"op_2_project_0_records_out_total":  int64(3),
			//
			//		"sink_mockSink_0_exceptions_total":  int64(0),
			//		"sink_mockSink_0_records_in_total":  int64(3),
			//		"sink_mockSink_0_records_out_total": int64(3),
			//	},
		}, {
			Name: `TestRestRule11`,
			Sql:  `SELECT PatchMessage(message_id, text)->text as message FROM mes`,
			R: [][]map[string]interface{}{
				{{
					"message": "message1",
				}},
				{{
					"message": "message2",
				}},
				{{
					"message": "message3",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule12`,
			Sql:  `SELECT objectDetectFromRest(*) AS res FROM optional_commands`,
			R: [][]map[string]interface{}{
				{{
					"res": map[string]interface{}{
						"image":  "my image1",
						"result": " success",
						"type":   "S",
					},
				}},
				{{
					"res": map[string]interface{}{
						"image":  "my image2",
						"result": " success",
						"type":   "S",
					},
				}},
				{{
					"res": map[string]interface{}{
						"image":  "my image3",
						"result": " success",
						"type":   "S",
					},
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		},
	}
	topotest.HandleStream(true, streamList, t)
	topotest.DoRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}

func jsonOut(w http.ResponseWriter, out interface{}) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	err := enc.Encode(out)
	// Problems encoding
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

type Resolver map[string]reflect.Value

func (r Resolver) Resolve(name string, _ []reflect.Value) (reflect.Value, error) {
	return r[name], nil
}

func SayHello(name string) map[string]interface{} {
	return map[string]interface{}{
		"message": name,
	}
}

func get_feature(img []byte) map[string]interface{} {
	l := len(string(img))
	return map[string]interface{}{
		"feature": []map[string]interface{}{
			{
				"features": []float64{-1.444, 2.55452, 5.121},
				"box": map[string]interface{}{
					"x": 153,
					"y": 107,
					"w": 174,
					"h": 100 + l,
				},
			}, {
				"features": []float64{1.444, -2.55452, -5.121},
				"box": map[string]interface{}{
					"x": 257,
					"y": 92,
					"w": 169,
					"h": 208,
				},
			},
		},
	}
}

func object_detection(command string, image string) map[string]interface{} {
	out := map[string]interface{}{
		"info":   command,
		"code":   200,
		"image":  image,
		"result": command + " success",
		"type":   "S",
	}
	return out
}

func getStatus() bool {
	return true
}

type server struct {
	UnimplementedGreeterServer
}

func (s *server) SayHello(_ context.Context, in *HelloRequest) (*HelloReply, error) {
	return &HelloReply{Message: in.GetName()}, nil
}

func (s *server) ObjectDetection(_ context.Context, in *ObjectDetectionRequest) (*ObjectDetectionResponse, error) {
	return &ObjectDetectionResponse{
		Info:   in.Cmd,
		Code:   200,
		Image:  in.Base64Img,
		Result: in.Cmd + " success",
		Type:   "S",
	}, nil
}

func (s *server) GetFeature(_ context.Context, v *wrappers.BytesValue) (*FeatureResponse, error) {
	l := len(string(v.Value))
	return &FeatureResponse{
		Feature: []*FeatureResult{
			{
				Features: []float32{-1.444, 2.55452, 5.121},
				Box: &Box{
					X: 153,
					Y: 107,
					W: 174,
					H: int32(100 + l),
				},
			},
			{
				Features: []float32{1.444, -2.55452, -5.121},
				Box: &Box{
					X: 257,
					Y: 92,
					W: 169,
					H: 208,
				},
			},
		},
	}, nil
}

func (s *server) GetStatus(context.Context, *empty.Empty) (*wrappers.BoolValue, error) {
	return &wrappers.BoolValue{Value: true}, nil
}

func TestGrpcService(t *testing.T) {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		kconf.Log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	RegisterGreeterServer(s, &server{})
	go func() {
		if err := s.Serve(lis); err != nil {
			kconf.Log.Fatalf("failed to serve: %v", err)
		}
	}()
	defer s.Stop()

	// Reset
	streamList := []string{"helloStr", "commands", "fakeBin"}
	topotest.HandleStream(false, streamList, t)
	// Data setup
	tests := []topotest.RuleTest{
		{
			Name: `TestRestRule1`,
			Sql:  `SELECT helloFromGrpc(name) as wc FROM helloStr`,
			R: [][]map[string]interface{}{
				{{
					"wc": map[string]interface{}{
						"message": "world",
					},
				}},
				{{
					"wc": map[string]interface{}{
						"message": "golang",
					},
				}},
				{{
					"wc": map[string]interface{}{
						"message": "peacock",
					},
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule2`,
			Sql:  `SELECT objectDetectFromGrpc(cmd, base64_img)->result FROM commands`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": "get success",
				}},
				{{
					"kuiper_field_0": "detect success",
				}},
				{{
					"kuiper_field_0": "delete success",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule3`,
			Sql:  `SELECT getFeatureFromGrpc(self)->feature[0]->box->h FROM fakeBin`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": float64(106), // Convert by the testing tool
				}},
				{{
					"kuiper_field_0": float64(107),
				}},
				{{
					"kuiper_field_0": float64(108),
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule4`,
			Sql:  `SELECT getStatusFromGrpc(), cmd FROM commands`,
			R: [][]map[string]interface{}{
				{{
					"getStatusFromGrpc": true,
					"cmd":               "get",
				}},
				{{
					"getStatusFromGrpc": true,
					"cmd":               "detect",
				}},
				{{
					"getStatusFromGrpc": true,
					"cmd":               "delete",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule5`,
			Sql:  `SELECT objectDetectFromGrpc(*) -> image AS res FROM optional_commands`,
			R: [][]map[string]interface{}{
				{{
					"res": "my image1",
				}},
				{{
					"res": "my image2",
				}},
				{{
					"res": "my image3",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		},
	}
	topotest.HandleStream(true, streamList, t)
	topotest.DoRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}

func TestSchemalessService(t *testing.T) {
	// mock server, the port is set in the sample.json
	l, err := net.Listen("tcp", "127.0.0.1:51234")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	count := 0
	router := mux.NewRouter()
	router.HandleFunc("/SayHello", func(w http.ResponseWriter, r *http.Request) {
		body := &RestHelloRequest{}
		err := json.NewDecoder(r.Body).Decode(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		out := &RestHelloReply{Message: body.Name}
		jsonOut(w, out)
	}).Methods(http.MethodPost)
	router.HandleFunc("/object_detection", func(w http.ResponseWriter, r *http.Request) {
		req := &ObjectDetectRequest{}
		err := json.NewDecoder(r.Body).Decode(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		if req.Image == "" {
			http.Error(w, "image is not found", http.StatusBadRequest)
		}
		out := &ObjectDetectResponse{
			Info:   req.Command,
			Code:   200,
			Image:  req.Image,
			Result: req.Command + " success",
			Type:   "S",
		}
		jsonOut(w, out)
	}).Methods(http.MethodPost)
	router.HandleFunc("/getStatus", func(w http.ResponseWriter, r *http.Request) {
		result := count%2 == 0
		count++
		io.WriteString(w, fmt.Sprintf("%v", result))
	}).Methods(http.MethodPost)
	router.HandleFunc("/RestEncodedJson", func(w http.ResponseWriter, r *http.Request) {
		req := &EncodedRequest{}
		err := json.NewDecoder(r.Body).Decode(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		io.WriteString(w, req.Name)
	}).Methods(http.MethodPost)
	router.HandleFunc("/bookshelf/v1/shelves", func(w http.ResponseWriter, r *http.Request) {
		req := &SchemalessShelfMessage{}
		err := json.NewDecoder(r.Body).Decode(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		if req.Id == 0 || req.Theme == "" {
			http.Error(w, "empty request", http.StatusBadRequest)
		}
		out := ShelfMessageOut{Id: req.Id, Theme: req.Theme}
		jsonOut(w, out)
	}).Methods(http.MethodPost)
	server := httptest.NewUnstartedServer(router)
	server.Listener.Close()
	server.Listener = l

	// Start the server.
	server.Start()

	defer server.Close()
	// Reset
	streamList := []string{"helloStr", "schemaless_commands", "shelves"}
	topotest.HandleStream(false, streamList, t)
	// Data setup
	tests := []topotest.RuleTest{
		{
			Name: `TestRestRule1`,
			Sql:  `SELECT tsschemaless("post", "/SayHello", *) as wc FROM helloStr`,
			R: [][]map[string]interface{}{
				{{
					"wc": map[string]interface{}{
						"message": "world",
					},
				}},
				{{
					"wc": map[string]interface{}{
						"message": "golang",
					},
				}},
				{{
					"wc": map[string]interface{}{
						"message": "peacock",
					},
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule2`,
			Sql:  `SELECT tsschemaless("post", "/object_detection", * EXCEPT(encoded_json))->result FROM schemaless_commands`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": "get success",
				}},
				{{
					"kuiper_field_0": "detect success",
				}},
				{{
					"kuiper_field_0": "delete success",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule3`,
			Sql:  `SELECT tsschemaless("post", "/object_detection", *)->result FROM schemaless_commands`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": "get success",
				}},
				{{
					"kuiper_field_0": "detect success",
				}},
				{{
					"kuiper_field_0": "delete success",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule4`,
			Sql:  `SELECT tsschemaless("post", "/getStatus"), cmd FROM schemaless_commands`,
			R: [][]map[string]interface{}{
				{{
					"tsschemaless": true,
					"cmd":          "get",
				}},
				{{
					"tsschemaless": false,
					"cmd":          "detect",
				}},
				{{
					"tsschemaless": true,
					"cmd":          "delete",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule5`,
			Sql:  `SELECT tsschemaless("post", "/RestEncodedJson", encoded_json) as name FROM schemaless_commands`,
			R: [][]map[string]interface{}{
				{{
					"name": "name1",
				}},
				{{
					"name": "name2",
				}},
				{{
					"name": "name3",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestRestRule6`,
			Sql:  `SELECT tsschemaless("post", "/bookshelf/v1/shelves", shelf)->theme as theme FROM shelves`,
			R: [][]map[string]interface{}{
				{{
					"theme": "tandra",
				}},
				{{
					"theme": "claro",
				}},
				{{
					"theme": "dark",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		},
	}
	topotest.HandleStream(true, streamList, t)
	topotest.DoRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}
