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

package node

import (
	"github.com/lf-edge/ekuiper/pkg/api"
)

type workerFunc func(item any) []any

func runWithOrder(ctx api.StreamContext, node *defaultSinkNode, numWorkers int, wf workerFunc) {
	workerChans := make([]chan any, numWorkers)
	workerOutChans := make([]chan []any, numWorkers)
	for i := range workerChans {
		workerChans[i] = make(chan any)
		workerOutChans[i] = make(chan []any)
	}

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		go worker(ctx, i, wf, workerChans[i], workerOutChans[i])
	}
	// start merger goroutine
	output := make(chan any)
	go merge(ctx, node, output, workerOutChans...)

	// Distribute input data to workers
	distribute(ctx, node, numWorkers, workerChans)
}

// Merge multiple channels into one preserving the order
func merge(ctx api.StreamContext, node *defaultSinkNode, output chan any, channels ...chan []any) {
	defer close(output)
	// Start a goroutine for each input channel
	for {
		for _, ch := range channels {
			select {
			case data := <-ch:
				for _, d := range data {
					node.Broadcast(d)
					switch dt := d.(type) {
					case error:
						node.statManager.IncTotalExceptions(dt.Error())
					default:
						node.statManager.IncTotalRecordsOut()
					}
				}
				node.statManager.IncTotalMessagesProcessed(1)
			case <-ctx.Done():
				ctx.GetLogger().Infof("merge done")
				return
			}
		}
	}
}

func distribute(ctx api.StreamContext, node *defaultSinkNode, numWorkers int, workerChans []chan any) {
	var counter int
	for {
		node.statManager.SetBufferLength(int64(len(node.input)))
		// Round-robin
		if counter == numWorkers {
			counter = 0
		}
		select {
		case <-ctx.Done():
			ctx.GetLogger().Infof("distribute done")
			return
		case item := <-node.input:
			ctx.GetLogger().Debugf("distributor receive %v", item)
			processed := false
			if item, processed = node.preprocess(item); processed {
				break
			}
			node.statManager.IncTotalRecordsIn()
			workerChans[counter] <- item
		}
		counter++
	}
}

func worker(ctx api.StreamContext, i int, wf workerFunc, inputRaw chan any, output chan []any) {
	for {
		select {
		case data := <-inputRaw:
			ctx.GetLogger().Debugf("worker %d received %v", i, data)
			select {
			case output <- wf(data):
			case <-ctx.Done():
				ctx.GetLogger().Debugf("worker %d done", i)
				return
			}
		case <-ctx.Done():
			ctx.GetLogger().Debugf("worker %d done", i)
			return
		}
	}
}
