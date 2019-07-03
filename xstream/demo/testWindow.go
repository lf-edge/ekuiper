package main

import (
	"engine/common"
	"engine/xsql"
	"engine/xsql/plans"
	"engine/xstream"
	"engine/xstream/collectors"
	"engine/xstream/extensions"
	"engine/xstream/operators"
	"strings"
)

func main() {

	log := common.Log

	demo1Stream, err := xsql.NewParser(strings.NewReader("CREATE STREAM demo (count bigint) WITH (datasource=\"demo\", FORMAT=\"AVRO\", KEY=\"USERID\")")).ParseCreateStreamStmt()
	//demo2Stream, err := xsql.NewParser(strings.NewReader("CREATE STREAM demo2 (abc bigint) WITH (datasource=\"demo2\", FORMAT=\"AVRO\", KEY=\"USERID\")")).ParseCreateStreamStmt()
	//stmt, err := xsql.NewParser(strings.NewReader("SELECT count FROM demo1 where demo1.count > 3")).Parse()
	if err != nil {
		log.Fatal("Failed to parse SQL for %s. \n", err)
	}

	tp := xstream.New()

	mqs1, err := extensions.NewWithName("srv1", "demo", "")
	if err != nil {
		log.Fatalf("Found error %s.\n", err)
		return
	}
	tp.AddSrc(mqs1)

	//mqs2, err := extensions.NewWithName("srv1", "demo2")
	//if err != nil {
	//	log.Fatalf("Found error %s.\n", err)
	//	return
	//}
	//tp.AddSrc(mqs2)

	preprocessorOp1 := xstream.Transform(&plans.Preprocessor{StreamStmt: demo1Stream}, "preprocessor1")
	tp.AddOperator([]xstream.Emitter{mqs1}, preprocessorOp1)

	//preprocessorOp2 := xstream.Transform(&plans.Preprocessor{StreamStmt: demo2Stream}, "preprocessor2")
	//tp.AddOperator([]xstream.Emitter{mqs2}, preprocessorOp2)

	//filterOp := xstream.Transform(&plans.FilterPlan{Condition: stmt.Condition}, "filter plan")
	//filterOp.SetConcurrency(3)
	//tp.AddOperator([]xstream.Emitter{preprocessorOp1, preprocessorOp2}, filterOp)
	//
	//projectOp := xstream.Transform(&plans.ProjectPlan{Fields: stmt.Fields}, "project plan")
	//tp.AddOperator([]xstream.Emitter{filterOp}, projectOp)

	//windowOp := operators.NewWindowOp("windowOp", &operators.WindowConfig{
	//	Type: operators.NO_WINDOW,
	//})

	//windowOp := operators.NewWindowOp("windowOp", &operators.WindowConfig{
	//	Type: operators.TUMBLING_WINDOW,
	//	Length: 30000,
	//})

	//windowOp := operators.NewWindowOp("windowOp", &operators.WindowConfig{
	//	Type: operators.HOPPING_WINDOW,
	//	Length: 20000,
	//	Interval: 10000,
	//})
	//
	//windowOp := operators.NewWindowOp("windowOp", &operators.WindowConfig{
	//	Type: operators.SLIDING_WINDOW,
	//	Length: 20000,
	//})

	windowOp := operators.NewWindowOp("windowOp", &operators.WindowConfig{
		Type: operators.SESSION_WINDOW,
		Length: 20000,
		Interval: 6000,
	})

	tp.AddOperator([]xstream.Emitter{preprocessorOp1}, windowOp)


	tp.AddSink([]xstream.Emitter{windowOp}, collectors.Func(func(data interface{}) error {
		log.Println("sink result %s", data)
		return nil
	}))

	if err := <-tp.Open(); err != nil {
		log.Fatal(err)
		return
	}
}
