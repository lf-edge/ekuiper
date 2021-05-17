package services

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	pb "github.com/emqx/kuiper/services/test/schemas/helloworld"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/topotest"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/msgpack-rpc/msgpack-rpc-go/rpc"
	"google.golang.org/grpc"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type HelloRequest struct {
	Name string `json:"name,omitempty"`
}

type HelloReply struct {
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

func TestRestService(t *testing.T) {
	// mock server, the port is set in the sample.json
	l, err := net.Listen("tcp", "127.0.0.1:51234")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	count := 0
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		defer r.Body.Close()
		var (
			out interface{}
		)
		switch path {
		case "/SayHello":
			body := &HelloRequest{}
			err := json.NewDecoder(r.Body).Decode(body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			out = &HelloReply{Message: body.Name}
		case "/object_detection":
			req := &ObjectDetectRequest{}
			err := json.NewDecoder(r.Body).Decode(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			if req.Image == "" {
				http.Error(w, "image is not found", http.StatusBadRequest)
			}
			out = &ObjectDetectResponse{
				Info:   req.Command,
				Code:   200,
				Image:  req.Image,
				Result: req.Command + " success",
				Type:   "S",
			}
		case "/getStatus":
			r := count%2 == 0
			count++
			io.WriteString(w, fmt.Sprintf("%v", r))
			return
		case "/RestEncodedJson":
			req := &EncodedRequest{}
			err := json.NewDecoder(r.Body).Decode(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			io.WriteString(w, req.Name)
			return
		default:
			http.Error(w, "path not supported", http.StatusBadRequest)
		}

		w.Header().Add("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		err = enc.Encode(out)
		// Problems encoding
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}))
	server.Listener.Close()
	server.Listener = l

	// Start the server.
	server.Start()

	defer server.Close()
	//Reset
	streamList := []string{"helloStr", "commands", "fakeBin"}
	topotest.HandleStream(false, streamList, t)
	//Data setup
	var tests = []topotest.RuleTest{
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
		},
	}
	topotest.HandleStream(true, streamList, t)
	topotest.DoRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}

type Resolver map[string]reflect.Value

func (self Resolver) Resolve(name string, _ []reflect.Value) (reflect.Value, error) {
	return self[name], nil
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

func TestMsgpackService(t *testing.T) {
	// mock server
	res := Resolver{"SayHello": reflect.ValueOf(SayHello), "object_detection": reflect.ValueOf(object_detection), "get_feature": reflect.ValueOf(get_feature), "getStatus": reflect.ValueOf(getStatus)}
	serv := rpc.NewServer(res, true, nil)
	l, _ := net.Listen("tcp", ":50000")
	serv.Listen(l)
	go serv.Run()
	// Comment out because the bug in the msgpack rpc
	// defer serv.Stop()

	//Reset
	streamList := []string{"helloStr", "commands", "fakeBin"}
	topotest.HandleStream(false, streamList, t)
	//Data setup
	var tests = []topotest.RuleTest{
		{
			Name: `TestRestRule1`,
			Sql:  `SELECT helloFromMsgpack(name) as wc FROM helloStr`,
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
			Sql:  `SELECT objectDetectFromMsgpack(*)->result FROM commands`,
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
			Sql:  `SELECT getFeatureFromMsgpack(self)->feature[0]->box->h FROM fakeBin`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": float64(106), //Convert by the testing tool
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
			//}, {
			//	Name: `TestRestRule4`,
			//	Sql:  `SELECT getStatusFromMsgpack(), command FROM commands`,
			//	R: [][]map[string]interface{}{
			//		{{
			//			"getStatusFromRest": true,
			//			"command": "get",
			//		}},
			//		{{
			//			"getStatusFromRest": true,
			//			"command": "detect",
			//		}},
			//		{{
			//			"getStatusFromRest": true,
			//			"command": "delete",
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
		},
	}
	topotest.HandleStream(true, streamList, t)
	topotest.DoRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}

type server struct {
	pb.UnimplementedGreeterServer
}

func (s *server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: in.GetName()}, nil
}

func (s *server) ObjectDetection(_ context.Context, in *pb.ObjectDetectionRequest) (*pb.ObjectDetectionResponse, error) {
	return &pb.ObjectDetectionResponse{
		Info:   in.Cmd,
		Code:   200,
		Image:  in.Base64Img,
		Result: in.Cmd + " success",
		Type:   "S",
	}, nil
}

func (s *server) GetFeature(_ context.Context, v *wrappers.BytesValue) (*pb.FeatureResponse, error) {
	l := len(string(v.Value))
	return &pb.FeatureResponse{
		Feature: []*pb.FeatureResult{
			{
				Features: []float32{-1.444, 2.55452, 5.121},
				Box: &pb.Box{
					X: 153,
					Y: 107,
					W: 174,
					H: int32(100 + l),
				},
			},
			{
				Features: []float32{1.444, -2.55452, -5.121},
				Box: &pb.Box{
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
		common.Log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	go func() {
		if err := s.Serve(lis); err != nil {
			common.Log.Fatalf("failed to serve: %v", err)
		}
	}()
	defer s.Stop()

	//Reset
	streamList := []string{"helloStr", "commands", "fakeBin"}
	topotest.HandleStream(false, streamList, t)
	//Data setup
	var tests = []topotest.RuleTest{
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
					"kuiper_field_0": float64(106), //Convert by the testing tool
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
		},
	}
	topotest.HandleStream(true, streamList, t)
	topotest.DoRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}
