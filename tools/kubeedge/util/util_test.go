package util

import (
	"kuiper/tools/kubeedge/common"
	"testing"
)

/*
func TestProcessDir(t *testing.T) {
	dirCommand, _ := filepath.Abs(`../sample/`)
	dirHistory, _ := filepath.Abs(`../.history/`)
	se := new(server)
	se.setDirCommand(dirCommand)
	se.setDirHistory(dirHistory)
	se.processDir()
	logs := se.getLogs()
	for _, v := range logs {
		fmt.Println(v)
	}
}
*/

func TestCall(t *testing.T) {
	conf := common.GetConf()
	conf.Ip = `127.0.0.1`
	conf.Port = 9081
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
				Method: `post`,
				Data:   struct{ sql string }{sql: `create stream stream2 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");`},
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
				Url:    `/streams`,
				Method: `put`,
			},
			exp: false,
		},
		{
			cmd: command{
				Url:    `/streams/stream1`,
				Method: `get`,
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/streams/stream2`,
				Method: `delete`,
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
				Method: `post`,
				Data: struct {
					id      string
					sql     string
					actions []struct{ log struct{} }
				}{
					id:  `ruler2`,
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
		{
			cmd: command{
				Url:    `/rules/rule1/restart`,
				Method: `post`,
			},
			exp: true,
		},
		{
			cmd: command{
				Url:    `/rules/rule1/status`,
				Method: `get`,
			},
			exp: true,
		},
	}

	for _, v := range tests {
		if v.exp != v.cmd.call() {
			t.Errorf("url:%s method:%s log:%s\n", v.cmd.Url, v.cmd.Method, v.cmd.getLog())
		}
	}
}
