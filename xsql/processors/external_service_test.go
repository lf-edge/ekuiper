package processors

import (
	"encoding/json"
	"github.com/emqx/kuiper/services"
	"github.com/emqx/kuiper/xstream/api"
	"net"
	"net/http"
	"net/http/httptest"
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

func TestServices(t *testing.T) {
	// mock server
	l, err := net.Listen("tcp", "127.0.0.1:51234")
	if err != nil {
		log.Fatal(err)
	}
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
			body := &ObjectDetectRequest{}
			err := json.NewDecoder(r.Body).Decode(body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			out = &ObjectDetectResponse{
				Info:   body.Command,
				Code:   200,
				Image:  body.Image,
				Result: body.Command + " success",
				Type:   "S",
			}
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
	m, _ := services.GetServiceManager()
	m.InitByFiles()
	//Reset
	streamList := []string{"helloStr", "commands"}
	handleStream(false, streamList, t)
	//Data setup
	var tests = []ruleTest{
		{
			name: `TestRestRule1`,
			sql:  `SELECT helloFromRest(name) as wc FROM helloStr`,
			r: [][]map[string]interface{}{
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
			m: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(3),
				"op_2_project_0_records_out_total":  int64(3),

				"sink_mockSink_0_exceptions_total":  int64(0),
				"sink_mockSink_0_records_in_total":  int64(3),
				"sink_mockSink_0_records_out_total": int64(3),
			},
		}, {
			name: `TestRestRule2`,
			sql:  `SELECT objectDetect(command, image)->result FROM commands`,
			r: [][]map[string]interface{}{
				{{
					"rengine_field_0": "get success",
				}},
				{{
					"rengine_field_0": "detect success",
				}},
				{{
					"rengine_field_0": "delete success",
				}},
			},
			m: map[string]interface{}{
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
	handleStream(true, streamList, t)
	doRuleTest(t, tests, 0, &api.RuleOption{
		BufferLength: 100,
		SendError:    true,
	})
}
