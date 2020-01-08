package nodes

import (
	"github.com/emqx/kuiper/common"
)

type CacheTuple struct {
	index int
	data interface{}
}

type Cache struct {
	//data and control channels
	in <-chan interface{}
	Out chan *CacheTuple
	Complete chan int
	done chan struct{}
	//states
	pending map[int]interface{}
	head    int
	tail    int   //pointers to the head and tail of the queue
	//configs
	limit int
	saveInterval int
}

func NewCache(in <-chan interface{},limit int, saveInterval int) *Cache{
	c :=  &Cache{
		in: in,
		Out: make(chan *CacheTuple, limit),
		Complete: make(chan int),
		done: make(chan struct{}),

		pending:      make(map[int]interface{}),
		head:         0,
		tail:         0,
		limit:        limit,
		saveInterval: saveInterval,
	}
	go c.run()
	return c
}

func (c *Cache) run(){
	//load cache

	ticker := common.GetTicker(c.saveInterval)
	// cache loop
	for{
		select {
		case item := <-c.in:
			c.pending[c.tail] = item
			c.tail++
			//non blocking until limit exceeded
			c.Out <- &CacheTuple{
				index: c.head,
				data:  c.pending[c.head],
			}
			c.head++
		case index := <-c.Complete:
			delete(c.pending, index)
		case <- ticker.GetC():
			if len(c.pending) == 0{
				c.head = 0
				c.tail = 0
			}else{
				//save to disk
				//copy map then use go func to save
			}
		case <-c.done:
			//save to disk
			return
		}
	}
}

func (c *Cache) Close(){
	c.done <- struct{}{}
}

func (c *Cache) Length() int{
	return len(c.pending)
}