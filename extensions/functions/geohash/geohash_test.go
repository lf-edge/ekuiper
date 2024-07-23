// Copyright 2024 EMQ Technologies Co., Ltd.
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

package main

import (
	"testing"

	"github.com/mmcloughlin/geohash"
	"github.com/stretchr/testify/assert"

	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestVal(t *testing.T) {
	err := GeohashEncode.Validate([]any{})
	assert.EqualError(t, err, "The geohashEncode function supports 2 parameters, but got 0")
	isAgg := GeohashEncode.IsAggregate()
	assert.False(t, isAgg)

	err = GeohashEncodeInt.Validate([]any{})
	assert.EqualError(t, err, "The geohashEncodeInt function supports 2 parameters, but got 0")
	isAgg = GeohashEncodeInt.IsAggregate()
	assert.False(t, isAgg)

	err = GeohashDecode.Validate([]any{})
	assert.EqualError(t, err, "The geohashDecode function supports 1 parameters, but got 0")
	isAgg = GeohashDecode.IsAggregate()
	assert.False(t, isAgg)

	err = GeohashDecodeInt.Validate([]any{})
	assert.EqualError(t, err, "The geohashDecodeInt function supports 1 parameters, but got 0")
	isAgg = GeohashDecodeInt.IsAggregate()
	assert.False(t, isAgg)

	err = GeohashBoundingBox.Validate([]any{})
	assert.EqualError(t, err, "The geohashBoundingBox function supports 1 parameters, but got 0")
	isAgg = GeohashBoundingBox.IsAggregate()
	assert.False(t, isAgg)

	err = GeohashBoundingBoxInt.Validate([]any{})
	assert.EqualError(t, err, "The geohashBoundingBoxInt function supports 1 parameters, but got 0")
	isAgg = GeohashBoundingBoxInt.IsAggregate()
	assert.False(t, isAgg)

	err = GeohashNeighbor.Validate([]any{})
	assert.EqualError(t, err, "The geohashNeighbor function supports 2 parameters, but got 0")
	isAgg = GeohashNeighbor.IsAggregate()
	assert.False(t, isAgg)

	err = GeohashNeighborInt.Validate([]any{})
	assert.EqualError(t, err, "The geohashNeighborInt function supports 2 parameters, but got 0")
	isAgg = GeohashNeighborInt.IsAggregate()
	assert.False(t, isAgg)

	err = GeohashNeighbors.Validate([]any{})
	assert.EqualError(t, err, "The geohashNeighbors function supports 1 parameters, but got 0")
	isAgg = GeohashNeighbors.IsAggregate()
	assert.False(t, isAgg)

	err = GeohashNeighborsInt.Validate([]any{})
	assert.EqualError(t, err, "The geohashNeighborsInt function supports 1 parameters, but got 0")
	isAgg = GeohashNeighborsInt.IsAggregate()
	assert.False(t, isAgg)
}

func TestString(t *testing.T) {
	ctx := mockContext.NewMockContext("TestEncodeDecode", "op")
	fctx := kctx.NewDefaultFuncContext(ctx, 2)
	la := 38.11
	lo := 120.55
	// Test encode
	r, s := GeohashEncode.Exec([]any{la, lo}, fctx)
	assert.True(t, s)
	assert.Equal(t, r, "wwv8z1tyfdgj")
	// Test wrong input
	e, s := GeohashEncode.Exec([]any{"la", lo}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a float, got la")

	e, s = GeohashEncode.Exec([]any{la, "lo"}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[1] is not a float, got lo")

	e, s = GeohashDecode.Exec([]any{la}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a string, got 38.11")

	e, s = GeohashDecode.Exec([]any{"la"}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "invalid character 'l'")

	e, s = GeohashBoundingBox.Exec([]any{la}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a string, got 38.11")

	e, s = GeohashBoundingBox.Exec([]any{"la"}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "invalid character 'l'")

	e, s = GeohashNeighbor.Exec([]any{la, "1"}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a string, got 38.11")

	e, s = GeohashNeighbor.Exec([]any{"la", "1"}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "invalid character 'l'")

	e, s = GeohashNeighbor.Exec([]any{r, 1}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[1] is not a string, got 1")

	e, s = GeohashNeighbor.Exec([]any{r, "1"}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[1] is valid, got 1")

	e, s = GeohashNeighbors.Exec([]any{la}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a string, got 38.11")

	e, s = GeohashNeighbors.Exec([]any{"la"}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "invalid character 'l'")
	// Test normal results
	p, s := GeohashDecode.Exec([]any{r}, fctx)
	assert.True(t, s)
	assert.Equal(t, position{Latitude: la, Longitude: lo}, p)

	b, s := GeohashBoundingBox.Exec([]any{r}, fctx)
	assert.True(t, s)
	assert.Equal(t, geohash.Box{MinLat: 38.10999998822808, MaxLat: 38.110000155866146, MinLng: 120.54999992251396, MaxLng: 120.55000025779009}, b)

	ns, s := GeohashNeighbors.Exec([]any{r}, fctx)
	assert.True(t, s)
	assert.Equal(t, []string{"wwv8z1tyfdgn", "wwv8z1tyfdgq", "wwv8z1tyfdgm", "wwv8z1tyfdgk", "wwv8z1tyfdgh", "wwv8z1tyfdfu", "wwv8z1tyfdfv", "wwv8z1tyfdfy"}, ns)

	n, s := GeohashNeighbor.Exec([]any{r, "North"}, fctx)
	assert.True(t, s)
	assert.Equal(t, "wwv8z1tyfdgn", n)
}

func TestInt(t *testing.T) {
	ctx := mockContext.NewMockContext("TestEncodeDecode", "op")
	fctx := kctx.NewDefaultFuncContext(ctx, 2)
	la := 38.11
	lo := 120.55
	// Test encode
	r, s := GeohashEncodeInt.Exec([]any{la, lo}, fctx)
	assert.True(t, s)
	assert.Equal(t, r, uint64(0xe7368f873e731f10))
	// Test wrong input
	e, s := GeohashEncodeInt.Exec([]any{"la", lo}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a float, got la")

	e, s = GeohashEncodeInt.Exec([]any{la, "lo"}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[1] is not a float, got lo")

	e, s = GeohashDecodeInt.Exec([]any{la}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a bigint, got 38.11")

	e, s = GeohashBoundingBoxInt.Exec([]any{la}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a bigint, got 38.11")

	e, s = GeohashNeighborsInt.Exec([]any{la}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a bigint, got 38.11")

	e, s = GeohashNeighborInt.Exec([]any{la}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a bigint, got 38.11")

	e, s = GeohashNeighborInt.Exec([]any{la, lo}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a bigint, got 38.11")

	e, s = GeohashNeighborInt.Exec([]any{la, "tt"}, fctx)
	assert.False(t, s)
	assert.EqualError(t, e.(error), "arg[0] is not a bigint, got 38.11")

	// normal exec
	p, s := GeohashDecodeInt.Exec([]any{r}, fctx)
	assert.True(t, s)
	assert.Equal(t, position{Latitude: 38.10999999, Longitude: 120.54999993}, p)

	b, s := GeohashBoundingBoxInt.Exec([]any{r}, fctx)
	assert.True(t, s)
	assert.Equal(t, geohash.Box{MinLat: 38.10999998822808, MaxLat: 38.1100000301376, MinLng: 120.54999992251396, MaxLng: 120.550000006333}, b)

	ns, s := GeohashNeighborsInt.Exec([]any{r}, fctx)
	assert.True(t, s)
	assert.Equal(t, []uint64{0xe7368f873e731f11, 0xe7368f873e731f13, 0xe7368f873e731f12, 0xe7368f873e731f07, 0xe7368f873e731f05, 0xe7368f873e731daf, 0xe7368f873e731dba, 0xe7368f873e731dbb}, ns)

	n, s := GeohashNeighborInt.Exec([]any{r, "North"}, fctx)
	assert.True(t, s)
	assert.Equal(t, uint64(0xe7368f873e731f11), n)
}
