package planner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

func TestParser(t *testing.T) {
	stmt, err := xsql.NewParser(strings.NewReader("select sum(a) as c, c + 1 as d from demo where c > 1")).Parse()
	require.NoError(t, err)
	require.NotNil(t, stmt)
}
