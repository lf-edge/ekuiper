// +build !windows

package processors

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"os"
	"path"
	"testing"
	"time"
)

//This cannot be run in Windows. And the plugins must be built to so before running this
//For Windows, run it in wsl with go test xsql/processors/extension_test.go xsql/processors/xsql_processor.go
func setup() *RuleProcessor {
	log := common.Log

	os.Remove(CACHE_FILE)

	dbDir, err := common.GetAndCreateDataLoc("test")
	if err != nil {
		log.Panic(err)
	}
	log.Infof("db location is %s", dbDir)

	p := NewStreamProcessor(path.Join(dbDir, "stream"))
	demo := `DROP STREAM ext`
	p.ExecStmt(demo)

	demo = "CREATE STREAM ext (count bigint) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"ext\")"
	_, err = p.ExecStmt(demo)
	if err != nil {
		panic(err)
	}

	demo = `DROP STREAM ext2`
	p.ExecStmt(demo)

	demo = "CREATE STREAM ext2 (count bigint) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"dedup\")"
	_, err = p.ExecStmt(demo)
	if err != nil {
		panic(err)
	}

	rp := NewRuleProcessor(dbDir)
	return rp
}

var CACHE_FILE = "cache"

//Test for source, sink, func and agg func extensions
//The .so files must be in the plugins folder
func TestExtensions(t *testing.T) {
	log := common.Log
	var tests = []struct {
		name      string
		rj        string
		minLength int
		maxLength int
	}{
		{
			name:      `$$test1`,
			rj:        "{\"sql\": \"SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext where count > 49\",\"actions\": [{\"file\":  {\"path\":\"" + CACHE_FILE + "\"}}]}",
			minLength: 5,
		}, {
			name:      `$$test2`,
			rj:        "{\"sql\": \"SELECT count(echo(count)) as c, echo(count) as e, countPlusOne(count) as p FROM ext2\",\"actions\": [{\"file\":  {\"path\":\"" + CACHE_FILE + "\"}}]}",
			maxLength: 2,
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	rp := setup()
	done := make(chan struct{})
	defer close(done)
	for i, tt := range tests {
		rp.ExecDrop(tt.name)
		rs, err := rp.ExecCreate(tt.name, tt.rj)
		if err != nil {
			t.Errorf("failed to create rule: %s.", err)
			continue
		}
		os.Create(CACHE_FILE)
		tp, err := rp.ExecInitRule(rs)
		if err != nil {
			t.Errorf("fail to init rule: %v", err)
			continue
		}

		go func() {
			select {
			case err := <-tp.Open():
				log.Println(err)
				tp.Cancel()
			case <-time.After(900 * time.Millisecond):
				tp.Cancel()
			}
		}()
		time.Sleep(1000 * time.Millisecond)
		log.Printf("exit main program after a second")
		results := getResults()
		if len(results) == 0 {
			t.Errorf("no result found")
			continue
		}
		log.Infof("get results %v", results)
		os.Remove(CACHE_FILE)
		var maps [][]map[string]interface{}
		for _, v := range results {
			var mapRes []map[string]interface{}
			err := json.Unmarshal([]byte(v), &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map")
				continue
			}
			maps = append(maps, mapRes)
		}

		if tt.minLength > 0 {
			if len(maps) < tt.minLength {
				t.Errorf("%d. %q\n\nresult length is smaller than minlength:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}

		if tt.maxLength > 0 {
			if len(maps) > tt.maxLength {
				t.Errorf("%d. %q\n\nresult length is bigger than maxLength:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}

		for _, r := range maps {
			if len(r) != 1 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			r := r[0]
			c := int((r["c"]).(float64))
			if c != 1 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			e := int((r["e"]).(float64))
			if e != 50 && e != 51 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
			p := int(r["p"].(float64))
			if p != 2 {
				t.Errorf("%d. %q\n\nresult mismatch:\n\ngot=%#v\n\n", i, tt.rj, maps)
				break
			}
		}
	}
}

func getResults() []string {
	f, err := os.Open(CACHE_FILE)
	if err != nil {
		panic(err)
	}
	result := make([]string, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		result = append(result, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	f.Close()
	return result
}
