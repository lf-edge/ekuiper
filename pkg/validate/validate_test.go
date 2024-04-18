package validate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRuleID(t *testing.T) {
	testcases := []struct {
		id  string
		err error
	}{
		{
			"abc",
			nil,
		},
		{
			"ABC",
			nil,
		},
		{
			"123",
			nil,
		},
		{
			"1/2",
			fmt.Errorf("ruleID:%s contains invalidChar:%v", "1/2", "/"),
		},
		{
			"1#2",
			fmt.Errorf("ruleID:%s contains invalidChar:%v", "1#2", "#"),
		},
		{
			"1%2",
			fmt.Errorf("ruleID:%s contains invalidChar:%v", "1%2", "%"),
		},
		{
			id:  "\t123",
			err: fmt.Errorf("ruleID: %v should be trimed", "\t123"),
		},
		{
			id:  "123\t",
			err: fmt.Errorf("ruleID: %v should be trimed", "123\t"),
		},
	}
	for _, tc := range testcases {
		require.Equal(t, tc.err, ValidateID(tc.id))
	}
}
