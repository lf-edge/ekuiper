package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	var v SliceVal
	require.True(t, v.IsEmpty())
	ok := v.SetByIndex(1, "test")
	require.False(t, ok)

	v = make(SliceVal, 10)
	require.True(t, v.IsEmpty())
	ok = v.SetByIndex(1, "test")
	require.True(t, ok)
	require.False(t, v.IsEmpty())
	ok = v.SetByIndex(-1, "test")
	require.False(t, ok)
	ok = v.SetByIndex(11, "test")
	require.False(t, ok)
}
