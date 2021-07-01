package service

import (
	"github.com/emqx/kuiper/internal/testx"
	"reflect"
	"testing"
)

var descriptors []descriptor

func init() {
	schemas := []*schemaInfo{
		{
			SchemaType: PROTOBUFF,
			SchemaFile: "hw.proto",
		},
	}
	descriptors = make([]descriptor, len(schemas))
	for i, sch := range schemas {
		d, err := parse(sch.SchemaType, sch.SchemaFile)
		if err != nil {
			panic(err)
		}
		descriptors[i] = d
	}
}

func TestConvertParams(t *testing.T) {
	tests := []struct {
		method  string
		params  []interface{}
		iresult []interface{}
		jresult []byte
		err     string
	}{
		{ //0
			method: "SayHello",
			params: []interface{}{
				"world",
			},
			iresult: []interface{}{
				"world",
			},
			jresult: []byte(`{"name":"world"}`),
		},
		{ //1
			method: "SayHello",
			params: []interface{}{
				map[string]interface{}{
					"name": "world",
				},
			},
			iresult: []interface{}{
				"world",
			},
			jresult: []byte(`{"name":"world"}`),
		},
		{ //2
			method: "SayHello",
			params: []interface{}{
				map[string]interface{}{
					"arbitrary": "world",
				},
			},
			err: "invalid type for string type field 'name': cannot convert map[string]interface {}(map[arbitrary:world]) to string",
		},
		{ //3
			method: "Compute",
			params: []interface{}{
				"rid", "uuid", "outlet", "path", []byte("data"), "extra",
			},
			iresult: []interface{}{
				"rid", "uuid", "outlet", "path", []byte("data"), "extra",
			},
			jresult: []byte(`{"rid":"rid","uuid":"uuid","outlet":"outlet","path":"path","data":"ZGF0YQ==","extra":"extra"}`),
		},
		{ //4
			method: "get_feature",
			params: []interface{}{
				[]byte("golang"),
			},
			iresult: []interface{}{
				[]byte("golang"),
			},
			jresult: []byte(`"Z29sYW5n"`),
		},
		//{ //5
		//	method: "get_similarity",
		//	params: []interface{}{
		//		[]float64{0.031646, -0.800592, -1.101858, -0.354359, 0.656587},
		//		[]float64{0.354359, 0.656587, -0.327047, 0.198284, -2.142494, 0.760160, 1.680131},
		//	},
		//	iresult: []interface{}{
		//		[]float32{0.031646, -0.800592, -1.101858, -0.354359, 0.656587},
		//		[]float32{0.354359, 0.656587, -0.327047, 0.198284, -2.142494, 0.760160, 1.680131},
		//	},
		//	jresult: []byte(`{"featureA":[0.031646,-0.800592,-1.101858,-0.354359,0.656587],"featureB":[0.354359,0.656587,-0.327047,0.198284,-2.142494,0.76016,1.680131]}`),
		//},
		{ // 6
			method: "RestEncodedJson",
			params: []interface{}{
				[]byte("{\"name\":\"encoded json\",\"size\":1}"),
			},
			iresult: []interface{}{
				"{\"name\":\"encoded json\",\"size\":1}",
			},
			jresult: []byte("{\"name\":\"encoded json\",\"size\":1}"),
		},
	}

	for i, descriptor := range descriptors {
		for j, tt := range tests {
			r, err := descriptor.(interfaceDescriptor).ConvertParams(tt.method, tt.params)
			if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
				t.Errorf("%d.%d : interface error mismatch:\n  exp=%s\n  got=%s\n\n", i, j, tt.err, err)
			} else if tt.err == "" && !reflect.DeepEqual(tt.iresult, r) {
				t.Errorf("%d.%d \n\ninterface result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, j, tt.iresult, r)
			}
			rj, err := descriptor.(jsonDescriptor).ConvertParamsToJson(tt.method, tt.params)
			if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
				t.Errorf("%d.%d : json error mismatch:\n  exp=%s\n  got=%s\n\n", i, j, tt.err, err)
			} else if tt.err == "" && !reflect.DeepEqual(tt.jresult, rj) {
				t.Errorf("%d.%d \n\njson result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, j, tt.jresult, rj)
			}
		}
	}
}

func TestConvertReturns(t *testing.T) {
	tests := []struct {
		method  string
		ireturn interface{}
		iresult interface{}
		ierr    string

		jreturn []byte
		jresult interface{}
		jerr    string
	}{
		{ // 0
			method:  "SayHello",
			ireturn: map[string]interface{}{"message": "world"},
			iresult: map[string]interface{}{"message": "world"},
			jreturn: []byte(`{"message":"world"}`),
			jresult: map[string]interface{}{"message": "world"},
		},
		{ // 1
			method:  "SayHello",
			ireturn: map[string]interface{}{"message": 65},
			ierr:    "invalid type of return value for 'message': cannot convert int(65) to string",
			jreturn: []byte(`{"message":65}`),
			jerr:    "invalid type of return value for 'message': cannot convert float64(65) to string",
		},
		//{
		//	method: "SayHello",
		//	ireturn: map[string]interface{}{
		//		"mess":"world",
		//	},
		//	jreturn: []byte(`{"mess":"world"}`),
		//err: "invalid type for field 'message', expect string but got int)",
		//},
		{ // 2
			method: "Compute",
			ireturn: map[string]interface{}{
				"code": int64(200),
				"msg":  "success",
			},
			iresult: map[string]interface{}{
				"code": int64(200),
				"msg":  "success",
			},
			jreturn: []byte(`{"code":200,"msg":"success"}`),
			jresult: map[string]interface{}{
				"code": int64(200),
				"msg":  "success",
			},
		},
		{
			method: "get_feature",
			ireturn: map[string]interface{}{"feature": []interface{}{ //TODO check msgpack result
				map[string]interface{}{
					"box":      map[string]interface{}{"x": int32(55), "y": int32(65), "w": int32(33), "h": int32(69)},
					"features": []float32{0.031646, -0.800592, -1.101858, -0.354359, 0.656587},
				},
				map[string]interface{}{
					"box":      map[string]interface{}{"x": int32(987), "y": int32(66), "w": int32(66), "h": int32(55)},
					"features": []float32{0.354359, 0.656587, -0.327047, 0.198284, -2.142494, 0.760160, 1.680131},
				},
			}},
			iresult: map[string]interface{}{
				"feature": []map[string]interface{}{
					{
						"box":      map[string]interface{}{"x": int64(55), "y": int64(65), "w": int64(33), "h": int64(69)},
						"features": []float64{float64(float32(0.031646)), float64(float32(-0.800592)), float64(float32(-1.101858)), float64(float32(-0.354359)), float64(float32(0.656587))},
					},
					{
						"box":      map[string]interface{}{"x": int64(987), "y": int64(66), "w": int64(66), "h": int64(55)},
						"features": []float64{float64(float32(0.354359)), float64(float32(0.656587)), float64(float32(-0.327047)), float64(float32(0.198284)), float64(float32(-2.142494)), float64(float32(0.760160)), float64(float32(1.680131))},
					},
				},
			},
			jreturn: []byte(`{"feature":[{"box":{"x":55,"y":65,"w":33,"h":69},"features":[0.031646, -0.800592, -1.101858, -0.354359, 0.656587]},{"box":{"x":987,"y":66,"w":66,"h":55},"features":[0.354359, 0.656587, -0.327047, 0.198284, -2.142494, 0.760160, 1.680131]}]}`),
			jresult: map[string]interface{}{
				"feature": []map[string]interface{}{
					{
						"box":      map[string]interface{}{"x": int64(55), "y": int64(65), "w": int64(33), "h": int64(69)},
						"features": []float64{0.031646, -0.800592, -1.101858, -0.354359, 0.656587},
					},
					{
						"box":      map[string]interface{}{"x": int64(987), "y": int64(66), "w": int64(66), "h": int64(55)},
						"features": []float64{0.354359, 0.656587, -0.327047, 0.198284, -2.142494, 0.760160, 1.680131},
					},
				},
			},
		},
		//{
		//	method:  "get_similarity",
		//	ireturn: float32(0.987),
		//	iresult: float64(float32(0.987)),
		//	jreturn: []byte(`{"response":0.987}`),
		//	jresult: map[string]interface{}{
		//		"response": 0.987,
		//	},
		//},
	}

	for i, descriptor := range descriptors {
		for j, tt := range tests {
			r, err := descriptor.(interfaceDescriptor).ConvertReturn(tt.method, tt.ireturn)
			if !reflect.DeepEqual(tt.ierr, testx.Errstring(err)) {
				t.Errorf("%d.%d : interface error mismatch:\n  exp=%s\n  got=%s\n\n", i, j, tt.ierr, err)
			} else if tt.ierr == "" && !reflect.DeepEqual(tt.iresult, r) {
				t.Errorf("%d.%d \n\ninterface result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, j, tt.iresult, r)
			}
			rj, err := descriptor.(jsonDescriptor).ConvertReturnJson(tt.method, tt.jreturn)
			if !reflect.DeepEqual(tt.jerr, testx.Errstring(err)) {
				t.Errorf("%d.%d : json error mismatch:\n  exp=%s\n  got=%s\n\n", i, j, tt.jerr, err)
			} else if tt.jerr == "" && !reflect.DeepEqual(tt.jresult, rj) {
				t.Errorf("%d.%d \n\njson result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, j, tt.jresult, rj)
			}
		}
	}
}
