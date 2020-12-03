package main

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql/processors"
	"path"
	"time"
)

func main() {
	log := common.Log
	dbDir, err := common.GetDataLoc()
	if err != nil {
		log.Panic(err)
	}
	log.Infof("db location is %s", dbDir)

	demo := `DROP STREAM ext`
	processors.NewStreamProcessor("stream").ExecStmt(demo)

	demo = "CREATE STREAM ext (count bigint) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\", TYPE=\"random\")"
	_, err = processors.NewStreamProcessor("stream").ExecStmt(demo)
	if err != nil {
		panic(err)
	}

	rp := processors.NewRuleProcessor(dbDir)
	rp.ExecDrop("$$test1")
	rs, err := rp.ExecCreate("$$test1", "{\"sql\": \"SELECT echo(count) FROM ext where count > 3\",\"actions\": [{\"memory\":  {}}]}")
	if err != nil {
		msg := fmt.Sprintf("failed to create rule: %s.", err)
		log.Printf(msg)
	}

	tp, err := rp.ExecInitRule(rs)
	if err != nil {
		log.Panicf("fail to init rule: %v", err)
	}

	go func() {
		select {
		case err := <-tp.Open():
			log.Println(err)
			tp.Cancel()
		}
	}()
	time.Sleep(5000000 * time.Millisecond)
	log.Infof("exit main program")
}
