package transform

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/props"
)

func TestProp(t *testing.T) {
	r := Prop("a")
	require.Equal(t, r, "a")
	props.SC.Set("a", "hello")
	r = Prop("a")
	require.Equal(t, r, "hello")
}
