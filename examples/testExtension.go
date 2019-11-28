package main

import (
	"engine/common"
	"engine/xsql/processors"
	"fmt"
	"path"
	"time"
)

func main() {
	log := common.Log
	dbDir, err := common.GetAndCreateDataLoc("test")
	if err != nil {
		log.Panic(err)
	}
	log.Infof("db location is %s", dbDir)

	demo := `DROP STREAM ext`
	processors.NewStreamProcessor(demo, path.Join(dbDir, "stream")).Exec()


	demo = "CREATE STREAM ext (count bigint) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\", TYPE=\"random\")"
	_, err = processors.NewStreamProcessor(demo, path.Join(dbDir, "stream")).Exec()
	if err != nil{
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
	if err != nil{
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
