package main

import (
	"github.com/lf-edge/ekuiper/tools/migration/util"
	"log"
	"os"
)

func main() {
	if 2 != len(os.Args) {
		log.Fatal("Please enter the correct path. For example: ./migration kuiper/bin/data")
	}

	if err := util.DataMigration(os.Args[1]); nil != err {
		log.Fatal(err)
	} else {
		log.Println("The data migration was successful.")
	}
}
