// Copyright 2026 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMetric(t *testing.T) {
	m, err := ParseMetric("")
	require.NoError(t, err)
	assert.Equal(t, MetricCosine, m)

	m, err = ParseMetric("l2")
	require.NoError(t, err)
	assert.Equal(t, MetricL2, m)

	m, err = ParseMetric(" ip ")
	require.NoError(t, err)
	assert.Equal(t, MetricIP, m)

	_, err = ParseMetric("hamming")
	assert.Error(t, err)
}

func TestParseVector(t *testing.T) {
	tests := []struct {
		name    string
		in      any
		want    []float64
		wantErr bool
	}{
		{name: "float64 slice", in: []float64{1, 2, 3}, want: []float64{1, 2, 3}},
		{name: "float32 slice", in: []float32{1.5, 2.5}, want: []float64{1.5, 2.5}},
		{name: "any numbers", in: []any{float64(1), float64(2.5)}, want: []float64{1, 2.5}},
		{name: "any mixed strings", in: []any{float64(1), "2.5"}, want: []float64{1, 2.5}},
		{name: "json array string", in: "[0.1, 0.2, 0.3]", want: []float64{0.1, 0.2, 0.3}},
		{name: "csv string", in: "0.1,0.2 ,0.3", want: []float64{0.1, 0.2, 0.3}},
		{name: "int slice", in: []int{1, 2}, want: []float64{1, 2}},
		{name: "nil", in: nil, wantErr: true},
		{name: "bad json", in: "[1,", wantErr: true},
		{name: "bad csv", in: "1,abc", wantErr: true},
		{name: "unsupported", in: 12345, wantErr: true},
		{name: "bad any element", in: []any{struct{}{}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseVector(tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFormatVector(t *testing.T) {
	assert.Equal(t, "[1,2.5,0.1]", FormatVector([]float64{1, 2.5, 0.1}))
	assert.Equal(t, "[]", FormatVector([]float64{}))
}

func TestValidateDimension(t *testing.T) {
	v := []float64{1, 2, 3}
	assert.NoError(t, ValidateDimension(v, 3))
	assert.NoError(t, ValidateDimension(v, 0))
	assert.Error(t, ValidateDimension(v, 4))
	assert.Error(t, ValidateDimension([]float64{math.NaN()}, 1))
	assert.Error(t, ValidateDimension([]float64{math.Inf(1)}, 1))
}

func TestCosineSimilarity(t *testing.T) {
	s, err := CosineSimilarity([]float64{1, 0}, []float64{1, 0})
	require.NoError(t, err)
	assert.InDelta(t, 1, s, 1e-9)

	s, err = CosineSimilarity([]float64{1, 0}, []float64{0, 1})
	require.NoError(t, err)
	assert.InDelta(t, 0, s, 1e-9)

	s, err = CosineSimilarity([]float64{1, 0}, []float64{-1, 0})
	require.NoError(t, err)
	assert.InDelta(t, -1, s, 1e-9)

	// zero magnitude vector yields 0
	s, err = CosineSimilarity([]float64{0, 0}, []float64{1, 1})
	require.NoError(t, err)
	assert.InDelta(t, 0, s, 1e-9)

	_, err = CosineSimilarity([]float64{1}, []float64{1, 2})
	assert.Error(t, err)
}

func TestL2Distance(t *testing.T) {
	d, err := L2Distance([]float64{0, 0}, []float64{3, 4})
	require.NoError(t, err)
	assert.InDelta(t, 5, d, 1e-9)

	_, err = L2Distance([]float64{1}, []float64{1, 2})
	assert.Error(t, err)
}

func TestInnerProduct(t *testing.T) {
	ip, err := InnerProduct([]float64{1, 2}, []float64{3, 4})
	require.NoError(t, err)
	assert.InDelta(t, 11, ip, 1e-9)
}

func TestScore(t *testing.T) {
	// cosine 0.6 -> 0.8
	assert.InDelta(t, 0.8, Score(MetricCosine, 0.6), 1e-9)
	// l2 1 -> 0.5
	assert.InDelta(t, 0.5, Score(MetricL2, 1), 1e-9)
	// ip 0 -> 0.5
	assert.InDelta(t, 0.5, Score(MetricIP, 0), 1e-9)
}

func TestNormalize(t *testing.T) {
	got := Normalize([]float64{3, 4})
	assert.InDelta(t, 0.6, got[0], 1e-9)
	assert.InDelta(t, 0.8, got[1], 1e-9)

	zero := Normalize([]float64{0, 0})
	assert.Equal(t, []float64{0, 0}, zero)
}
