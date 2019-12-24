package utils

type DynamicChannelBuffer struct {
	In chan interface{}
	Out chan interface{}
	buffer []interface{}
}

func NewDynamicChannelBuffer() *DynamicChannelBuffer {
	buffer := &DynamicChannelBuffer{
		In: make(chan interface{}),
		Out: make(chan interface{}),
		buffer: make([]interface{}, 0),
	}
	go buffer.run()
	return buffer
}

func (b *DynamicChannelBuffer) run() {
	for {
		if len(b.buffer) > 0 {
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
