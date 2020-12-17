package planner

import "github.com/emqx/kuiper/xsql"

func GetRefSources(node xsql.Node) []string {
	result := make(map[string]bool)
	keys := make([]string, 0, len(result))
	if node == nil {
		return keys
	}
	xsql.WalkFunc(node, func(n xsql.Node) {
		if f, ok := n.(*xsql.FieldRef); ok {
			result[string(f.StreamName)] = true
		}
	})
	for k := range result {
		keys = append(keys, k)
	}
	return keys
}
