package cast

import (
	"reflect"
	"strings"
	"testing"
)

func TestString2bytes(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "common",
			args: args{
				str: "abc",
			},
			want: []byte{'a', 'b', 'c'},
		},
		{
			name: "nil",
			args: args{
				str: "",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringToBytes(tt.args.str); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StringToBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

var (
	l   = 1024 * 1024
	str = strings.Repeat("a", l)
)

func BenchmarkStringToBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bt := []byte(str)
		if len(bt) != l {
			b.Fatal()
		}
	}
}

func BenchmarkStringToBytesUnsafe(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bt := StringToBytes(str)
		if len(bt) != l {
			b.Fatal()
		}
	}
}
