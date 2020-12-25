package util

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCall(t *testing.T) {
	var tests = []struct {
		cmd command
		exp bool
	}{
		{
			cmd: command{
				Url:    `/streams`,
				Method: `post`,
				Data:   struct{ sql string }{sql: `create stream stream1 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");`},
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/streams`,
				Method: `get`,
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/streams/stream1`,
				Method: `put`,
				Data:   struct{ sql string }{sql: `create stream stream1 (id bigint, name string) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");`},
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/rules`,
				Method: `post`,
				Data: struct {
					id      string
					sql     string
					actions []struct{ log struct{} }
				}{
					id:  `ruler1`,
					sql: `SELECT * FROM stream1`,
				},
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/rules`,
				Method: `get`,
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/rules/rule1`,
				Method: `get`,
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/rules/rule2`,
				Method: `delete`,
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/rules/rule1/stop`,
				Method: `post`,
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/rules/rule1/start`,
				Method: `post`,
			},
			exp: true,
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer ts.Close()

	for _, v := range tests {
		ret := v.cmd.call(ts.URL)
		if v.exp != ret {
			t.Errorf("url:%s method:%s log:%s\n", v.cmd.Url, v.cmd.Method, v.cmd.getLog())
		}
	}
}
