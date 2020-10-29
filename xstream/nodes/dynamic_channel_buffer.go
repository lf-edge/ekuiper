package nodes

import (
	"sync/atomic"

	"github.com/emqx/kuiper/xstream/api"
)

type DynamicChannelBuffer struct {
	limit  int64
	In     chan api.SourceTuple
	Out    chan api.SourceTuple
	buffer []api.SourceTuple
}

func NewDynamicChannelBuffer() *DynamicChannelBuffer {
	buffer := &DynamicChannelBuffer{
		In:     make(chan api.SourceTuple),
		Out:    make(chan api.SourceTuple),
		buffer: make([]api.SourceTuple, 0),
		limit:  102400,
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
			b.Out <- b.buffer[0]
			b.buffer = b.buffer[1:]
		} else if l > 0 {
			select {
			case b.Out <- b.buffer[0]:
				b.buffer = b.buffer[1:]
			case value := <-b.In:
				b.buffer = append(b.buffer, value)
			}
		} else {
			value := <-b.In
			b.buffer = append(b.buffer, value)
		}
	}
}

func (b *DynamicChannelBuffer) GetLength() int {
	return len(b.buffer)
}
