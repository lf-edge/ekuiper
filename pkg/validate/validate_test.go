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
			fmt.Errorf("ruleID:%s contains invalidChar", "1/2"),
		},
		{
			"1#2",
			fmt.Errorf("ruleID:%s contains invalidChar", "1#2"),
		},
		{
			"1%2",
			fmt.Errorf("ruleID:%s contains invalidChar", "1%2"),
		},
		{
			"1-2",
			nil,
		},
		{
			"1.2",
			fmt.Errorf("ruleID:%s contains invalidChar", "1.2"),
		},
		{
			"valid_id_123",
			nil,
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
		err := ValidateID(tc.id)
		if tc.err != nil {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}
