// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"fmt"

	"github.com/mmcloughlin/geohash"

	"github.com/lf-edge/ekuiper/pkg/api"
)

type (
	geohashEncode         struct{}
	geohashEncodeInt      struct{}
	geohashDecode         struct{}
	geohashDecodeInt      struct{}
	geohashBoundingBox    struct{}
	geohashBoundingBoxInt struct{}
	geohashNeighbor       struct{}
	geohashNeighborInt    struct{}
	geohashNeighbors      struct{}
	geohashNeighborsInt   struct{}
	position              struct {
		Longitude float64
		Latitude  float64
	}
)

var (
	GeohashEncode         geohashEncode
	GeohashEncodeInt      geohashEncodeInt
	GeohashDecode         geohashDecode
	GeohashDecodeInt      geohashDecodeInt
	GeohashBoundingBox    geohashBoundingBox
	GeohashBoundingBoxInt geohashBoundingBoxInt
	GeohashNeighbor       geohashNeighbor
	GeohashNeighborInt    geohashNeighborInt
	GeohashNeighbors      geohashNeighbors
	GeohashNeighborsInt   geohashNeighborsInt
	g_direction           = map[string]geohash.Direction{
		"North":     geohash.North,
		"NorthEast": geohash.NorthEast,
		"East":      geohash.East,
		"SouthEast": geohash.SouthEast,
		"South":     geohash.South,
		"SouthWest": geohash.SouthWest,
		"West":      geohash.West,
		"NorthWest": geohash.NorthWest,
	}
)

func (r *geohashEncode) IsAggregate() bool {
	return false
}

func (r *geohashEncodeInt) IsAggregate() bool {
	return false
}

func (r *geohashDecode) IsAggregate() bool {
	return false
}

func (r *geohashDecodeInt) IsAggregate() bool {
	return false
}

func (r *geohashBoundingBox) IsAggregate() bool {
	return false
}

func (r *geohashBoundingBoxInt) IsAggregate() bool {
	return false
}

func (r *geohashNeighbor) IsAggregate() bool {
	return false
}

func (r *geohashNeighborInt) IsAggregate() bool {
	return false
}

func (r *geohashNeighbors) IsAggregate() bool {
	return false
}

func (r *geohashNeighborsInt) IsAggregate() bool {
	return false
}

func (r *geohashEncode) Validate(args []interface{}) error {
	if len(args) != 2 {
		return fmt.Errorf("The geohashEncode function supports 2 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashEncodeInt) Validate(args []interface{}) error {
	if len(args) != 2 {
		return fmt.Errorf("The geohashEncodeInt function supports 2 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashDecode) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("The geohashDecode function supports 1 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashDecodeInt) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("The geohashDecodeInt function supports 1 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashBoundingBox) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("The geohashBoundingBox function supports 1 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashBoundingBoxInt) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("The geohashBoundingBoxInt function supports 1 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashNeighbor) Validate(args []interface{}) error {
	if len(args) != 2 {
		return fmt.Errorf("The geohashNeighbor function supports 2 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashNeighborInt) Validate(args []interface{}) error {
	if len(args) != 2 {
		return fmt.Errorf("The geohashNeighborInt function supports 2 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashNeighbors) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("The geohashNeighbors function supports 1 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashNeighborsInt) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("The geohashNeighborsInt function supports 1 parameters, but got %d", len(args))
	}
	return nil
}

func (r *geohashEncode) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	la, ok := args[0].(float64)
	if !ok {
		return fmt.Errorf("arg[0] is not a float, got %v", args[0]), false
	}
	lo, ok := args[1].(float64)
	if !ok {
		return fmt.Errorf("arg[1] is not a float, got %v", args[1]), false
	}
	return geohash.Encode(la, lo), true
}

func (r *geohashEncodeInt) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	la, ok := args[0].(float64)
	if !ok {
		return fmt.Errorf("arg[0] is not a float, got %v", args[0]), false
	}
	lo, ok := args[1].(float64)
	if !ok {
		return fmt.Errorf("arg[1] is not a float, got %v", args[1]), false
	}
	return geohash.EncodeInt(la, lo), true
}

func (r *geohashDecode) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	hash, ok := args[0].(string)
	if !ok || 0 == len(hash) {
		return fmt.Errorf("arg[0] is not a string, got %v", args[0]), false
	}
	if err := geohash.Validate(hash); nil != err {
		return err, false
	}
	la, lo := geohash.Decode(hash)
	return position{Longitude: lo, Latitude: la}, true
}

func (r *geohashDecodeInt) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	hash, ok := args[0].(uint64)
	if !ok || 0 > hash {
		return fmt.Errorf("arg[0] is not a bigint, got %v", args[0]), false
	}
	la, lo := geohash.DecodeInt(hash)
	return position{Longitude: lo, Latitude: la}, true
}

func (r *geohashBoundingBox) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	hash, ok := args[0].(string)
	if !ok || 0 == len(hash) {
		return fmt.Errorf("arg[0] is not a string, got %v", args[0]), false
	}
	if err := geohash.Validate(hash); nil != err {
		return err, false
	}
	return geohash.BoundingBox(hash), true
}

func (r *geohashBoundingBoxInt) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	hash, ok := args[0].(uint64)
	if !ok || 0 > hash {
		return fmt.Errorf("arg[0] is not a bigint, got %v", args[0]), false
	}
	return geohash.BoundingBoxInt(hash), true
}

func (r *geohashNeighbor) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	hash, ok := args[0].(string)
	if !ok || 0 == len(hash) {
		return fmt.Errorf("arg[0] is not a string, got %v", args[0]), false
	}
	if err := geohash.Validate(hash); nil != err {
		return err, false
	}
	var directionCode geohash.Direction
	direction, ok := args[1].(string)
	if !ok || 0 == len(direction) {
		return fmt.Errorf("arg[1] is not a string, got %v", args[1]), false
	} else {
		directionCode, ok = g_direction[direction]
		if !ok {
			return fmt.Errorf("arg[1] is valid, got %v", args[1]), false
		}

	}
	return geohash.Neighbor(hash, directionCode), true
}

func (r *geohashNeighborInt) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	hash, ok := args[0].(uint64)
	if !ok || 0 > hash {
		return fmt.Errorf("arg[0] is not a bigint, got %v", args[0]), false
	}
	var directionCode geohash.Direction
	direction, ok := args[1].(string)
	if !ok || 0 == len(direction) {
		return fmt.Errorf("arg[1] is not a string, got %v", args[1]), false
	} else {
		directionCode, ok = g_direction[direction]
		if !ok {
			return fmt.Errorf("arg[1] is valid, got %v", args[1]), false
		}
	}
	return geohash.NeighborInt(hash, directionCode), true
}

func (r *geohashNeighbors) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	hash, ok := args[0].(string)
	if !ok || 0 == len(hash) {
		return fmt.Errorf("arg[0] is not a string, got %v", args[0]), false
	}
	if err := geohash.Validate(hash); nil != err {
		return err, false
	}
	return geohash.Neighbors(hash), true
}

func (r *geohashNeighborsInt) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	hash, ok := args[0].(uint64)
	if !ok || 0 > hash {
		return fmt.Errorf("arg[0] is not a bigint, got %v", args[0]), false
	}
	return geohash.NeighborsInt(hash), true
}
