package server

import (
	"fmt"
	"path/filepath"
	"testing"
)

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

func TestCall(t *testing.T) {
	var commands = []command{
		{
			Url:    `/streams`,
			Method: `post`,
			Data:   struct{ sql string }{sql: `create stream stream1 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");`},
		},
		{
			Url:    `/streams`,
			Method: `post`,
			Data:   struct{ sql string }{sql: `create stream stream2 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");`},
		},
		{
			Url:    `/streams`,
			Method: `get`,
		},
		{
			Url:    `/streams/stream1`,
			Method: `get`,
		},
		{
			Url:    `/streams/stream2`,
			Method: `delete`,
		},
		{
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
		{
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
		{
			Url:    `/rules`,
			Method: `get`,
		},
		{
			Url:    `/rules/rule1`,
			Method: `get`,
		},
		{
			Url:    `/rules/rule2`,
			Method: `delete`,
		},
		{
			Url:    `/rules/rule1/stop`,
			Method: `post`,
		},
		{
			Url:    `/rules/rule1/start`,
			Method: `post`,
		},
		{
			Url:    `/rules/rule1/restart`,
			Method: `post`,
		},
		{
			Url:    `/rules/rule1/status`,
			Method: `get`,
		},
	}
	for _, command := range commands {
		command.call()
		fmt.Println(command.getLog())
	}
}
