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
	BadgerDir, err := common.GetAndCreateDataLoc("test")
	if err != nil {
		log.Panic(err)
	}
	log.Infof("badge location is %s", BadgerDir)

	demo := `DROP STREAM ext`
	processors.NewStreamProcessor(demo, path.Join(BadgerDir, "stream")).Exec()


	demo = "CREATE STREAM ext (count bigint) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\", TYPE=\"RandomSource\")"
	_, err = processors.NewStreamProcessor(demo, path.Join(BadgerDir, "stream")).Exec()
	if err != nil{
		panic(err)
	}

	sql := "SELECT count FROM ext where ext.count > 3"

	tp, err := processors.NewRuleProcessor(BadgerDir).ExecQuery("$$test", sql)
	if err != nil {
		msg := fmt.Sprintf("failed to create query: %s.", err)
		log.Printf(msg)
	}
	time.Sleep(5000000 * time.Millisecond)
	tp.Cancel()
	log.Infof("exit main program")
}
