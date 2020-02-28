package main

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"strings"
)

func main() {
	stmt, _ := xsql.NewParser(strings.NewReader("SELECT id1 FROM src1 left join src2 on src1.f1->cid = src2.f2->cid")).Parse()

	var srcs []string
	xsql.WalkFunc(stmt.Joins, func(node xsql.Node) {
		if f, ok := node.(*xsql.FieldRef); ok {
			if string(f.StreamName) == "" {
				return
			}
			srcs = append(srcs, string(f.StreamName))
		}
	})

	for _, src := range srcs {
		fmt.Println(src)
	}
}
