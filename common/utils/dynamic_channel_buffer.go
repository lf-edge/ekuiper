package utils

type DynamicChannelBuffer struct {
	In chan interface{}
	Out chan interface{}
	buffer []interface{}
	limit int
}

func NewDynamicChannelBuffer() *DynamicChannelBuffer {
	buffer := &DynamicChannelBuffer{
		In: make(chan interface{}),
		Out: make(chan interface{}),
		buffer: make([]interface{}, 0),
		limit: 102400,
	}
	go buffer.run()
	return buffer
}

func (b *DynamicChannelBuffer) SetLimit(limit int){
	if limit > 0 {
		b.limit = limit
	}
}

func (b *DynamicChannelBuffer) run() {
	for {
		l := len(b.buffer)
		if l >= b.limit{
			b.Out <- b.buffer[0]
			b.buffer = b.buffer[1:]
		}else if l > 0 {
			select {
			case b.Out <- b.buffer[0]:
				b.buffer = b.buffer[1:]
			case value := <- b.In:
				b.buffer = append(b.buffer, value)
			}
		} else {
			value := <- b.In
			b.buffer = append(b.buffer, value)
		}
	}
}

func (b *DynamicChannelBuffer) GetLength() int {
	return len(b.buffer)
}
