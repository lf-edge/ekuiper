// Copyright 2023 EMQ Technologies Co., Ltd.
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

//go:build test

package can

import (
	"fmt"
	"time"

	"github.com/ngjaying/can"
	"github.com/ngjaying/can/pkg/socketcan"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

func (s *source) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	e, err := socketcan.NewEmulator()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Emulator started on %s, %s\n", e.Addr().Network(), e.Addr().String())
	go func() {
		err = e.Run(ctx)
		if err != nil {
			panic(err)
		}
	}()
	// Use ctx so that we don't need to close the connection manually
	conn, err := socketcan.DialContext(ctx, s.conf.Network, s.conf.Address)
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	recv := socketcan.NewReceiver(conn)

	go func() {
		tx := socketcan.NewTransmitter(conn)
		id := 1
		for id < 4 {
			time.Sleep(1 * time.Second)
			txFrame := can.Frame{ID: uint32(id), Length: 4, Data: can.Data{1, 2, 3, 4}}
			fmt.Printf("Transmitting %d\n", id)
			err = tx.TransmitFrame(ctx, txFrame)
			if err != nil {
				fmt.Println(err)
				time.Sleep(10 * time.Second)
			}
			id++
		}
	}()
	// The scan will exit when the context is done because we pass in ctx
	for {
		bytes, ok := recv.Scan()
		if !ok {
			ctx.GetLogger().Infof("receiver scan is done")
			return
		}
		// result, e := ctx.Decode(bytes)
		result, e := mockMap(bytes)
		if e != nil {
			ctx.GetLogger().Errorf("Invalid data format, cannot decode %x with error %s", bytes, e)
		} else {
			select {
			case consumer <- api.NewDefaultSourceTuple(result, nil):
			case <-ctx.Done():
				return
			}
		}
	}
}

func mockMap(bytes []byte) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	result["data"] = bytes
	return result, nil
}
