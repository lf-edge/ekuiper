package xsql

import (
	"bytes"
	"strings"
	"testing"
)

var l = 10000

func BenchmarkBytesBufferToString(b *testing.B) {
	buf := bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			_, err := buf.WriteRune('1')
			if err != nil {
				b.Fatal(err)
			}
		}
		str := buf.String()
		if len(str) != l {
			b.Fatal()
		}
		buf.Reset()
	}
}

func BenchmarkStringBuilderToString(b *testing.B) {
	buf := strings.Builder{}
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			_, err := buf.WriteRune('1')
			if err != nil {
				b.Fatal(err)
			}
		}
		str := buf.String()
		if len(str) != l {
			b.Fatal()
		}
		buf.Reset()
	}
}

func BenchmarkBytesBufferToStringWithNewBuffer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		for j := 0; j < l; j++ {
			_, err := buf.WriteRune('1')
			if err != nil {
				b.Fatal(err)
			}
		}
		str := buf.String()
		if len(str) != l {
			b.Fatal()
		}
	}
}
