package main

import (
	"testing"
)

func TestRGBA(t *testing.T) {
	tests := []struct {
		input     grayscaleFloat
		expectedR uint32
		expectedG uint32
		expectedB uint32
		expectedA uint32
	}{
		{0, 0, 0, 0, 0xffff},
		{0.5, 0x7fff, 0x7fff, 0x7fff, 0xffff},
		{1, 0xffff, 0xffff, 0xffff, 0xffff},
		{1.5, 0xffff, 0xffff, 0xffff, 0xffff},
		{-0.5, 0xffff, 0xffff, 0xffff, 0xffff},
		{-1, 0xffff, 0xffff, 0xffff, 0xffff},
	}

	for _, tt := range tests {
		r, g, b, a := tt.input.RGBA()
		if r != tt.expectedR || g != tt.expectedG || b != tt.expectedB || a != tt.expectedA {
			t.Errorf("For input %f, expected (r,g,b,a) = (%d,%d,%d,%d), got (%d,%d,%d,%d)", tt.input, tt.expectedR, tt.expectedG, tt.expectedB, tt.expectedA, r, g, b, a)
		}
	}
}
