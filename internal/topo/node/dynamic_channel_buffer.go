package node

import (
	"github.com/lf-edge/ekuiper/pkg/api"
	"sync/atomic"
)

type DynamicChannelBuffer struct {
	limit  int64
	In     chan api.SourceTuple
	Out    chan api.SourceTuple
	buffer []api.SourceTuple
	done   chan bool
}

func NewDynamicChannelBuffer() *DynamicChannelBuffer {
	buffer := &DynamicChannelBuffer{
		In:     make(chan api.SourceTuple),
		Out:    make(chan api.SourceTuple),
		buffer: make([]api.SourceTuple, 0),
		limit:  102400,
		done:   make(chan bool, 1),
	}
	go buffer.run()
	return buffer
}

func (b *DynamicChannelBuffer) SetLimit(limit int) {
	if limit > 0 {
		atomic.StoreInt64(&b.limit, int64(limit))
	}
}

func (b *DynamicChannelBuffer) run() {
	for {
		l := len(b.buffer)
		if int64(l) >= atomic.LoadInt64(&b.limit) {
			select {
			case b.Out <- b.buffer[0]:
				b.buffer = b.buffer[1:]
			case <-b.done:
				return
			}
		} else if l > 0 {
			select {
			case b.Out <- b.buffer[0]:
				b.buffer = b.buffer[1:]
			case value := <-b.In:
				b.buffer = append(b.buffer, value)
			case <-b.done:
				return
			}
		} else {
			select {
			case value := <-b.In:
				b.buffer = append(b.buffer, value)
			case <-b.done:
				return
			}
		}
	}
}

func (b *DynamicChannelBuffer) GetLength() int {
	return len(b.buffer)
}

func (b *DynamicChannelBuffer) Close() {
	b.done <- true
}
