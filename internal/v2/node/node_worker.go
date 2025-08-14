package node

import (
	"context"
	"fmt"
	"sync"
)

type OrderNodeMessageHandler struct {
	In              chan *NodeMessage
	Out             chan *NodeMessage
	ctx             context.Context
	workers         []*orderNodeMessageWorker
	workerExitCount int
	graceExitSignal chan struct{}
	quickExitSignal chan struct{}
	wg              *sync.WaitGroup
	status          int
	sendIndex       int
	recvIndex       int
}

func NewOrderNodeMessageHandler(ctx context.Context, concurrency int, workerHandler func(ctx context.Context, data *NodeMessage) *NodeMessage) *OrderNodeMessageHandler {
	handler := &OrderNodeMessageHandler{
		In:              make(chan *NodeMessage, 16),
		Out:             make(chan *NodeMessage, 16),
		ctx:             ctx,
		graceExitSignal: make(chan struct{}),
		quickExitSignal: make(chan struct{}),
		workers:         make([]*orderNodeMessageWorker, 0),
		wg:              &sync.WaitGroup{},
	}
	for i := 0; i < concurrency; i++ {
		worker := &orderNodeMessageWorker{
			In:              make(chan *NodeMessage, 16),
			Out:             make(chan *NodeMessage, 16),
			ctx:             ctx,
			handler:         workerHandler,
			quickExitSignal: handler.quickExitSignal,
			wg:              handler.wg,
		}
		handler.wg.Add(1)
		go worker.run()
		handler.workers = append(handler.workers, worker)
	}
	handler.wg.Add(1)
	go handler.run()
	return handler
}

func (p *OrderNodeMessageHandler) QuickClose() {
	close(p.quickExitSignal)
	p.wg.Wait()
}

func (p *OrderNodeMessageHandler) GraceClose() {
	close(p.graceExitSignal)
	p.wg.Wait()
	fmt.Println("grace close")
}

func (p *OrderNodeMessageHandler) run() {
	defer func() {
		p.wg.Done()
	}()
	for {
		select {
		case <-p.quickExitSignal:
			return
		default:
			switch p.status {
			case 0:
				p.run0()
			case 1:
				p.run1()
			case 2:
				p.run2()
			case 3:
				return
			}
		}
	}
}

func (p *OrderNodeMessageHandler) run0() {
	select {
	case msg := <-p.In:
		p.workers[p.sendIndex].In <- msg
		p.sendIndex++
		if p.sendIndex == len(p.workers) {
			p.sendIndex = 0
		}
	case msg := <-p.workers[p.recvIndex].Out:
		p.Out <- msg
		p.recvIndex++
		if p.recvIndex == len(p.workers) {
			p.recvIndex = 0
		}
	case <-p.graceExitSignal:
		p.status = 1
		close(p.In)
	case <-p.quickExitSignal:
		return
	}
}

func (p *OrderNodeMessageHandler) run1() {
	select {
	case msg, ok := <-p.In:
		if !ok {
			for _, w := range p.workers {
				close(w.In)
			}
			p.status = 2
			return
		}
		p.workers[p.sendIndex].In <- msg
		p.sendIndex++
		if p.sendIndex == len(p.workers) {
			p.sendIndex = 0
		}
	case msg := <-p.workers[p.recvIndex].Out:
		p.Out <- msg
		p.recvIndex++
		if p.recvIndex == len(p.workers) {
			p.recvIndex = 0
		}
	case <-p.quickExitSignal:
		return
	}
}

func (p *OrderNodeMessageHandler) run2() {
	select {
	case msg, ok := <-p.workers[p.recvIndex].Out:
		if !ok {
			p.workerExitCount++
			if p.workerExitCount >= len(p.workers) {
				close(p.Out)
				p.status = 3
				return
			}
			return
		}
		p.Out <- msg
		p.recvIndex++
		if p.recvIndex == len(p.workers) {
			p.recvIndex = 0
		}
	case <-p.quickExitSignal:
		return
	}
}

type orderNodeMessageWorker struct {
	In              chan *NodeMessage
	Out             chan *NodeMessage
	ctx             context.Context
	handler         func(ctx context.Context, data *NodeMessage) *NodeMessage
	quickExitSignal chan struct{}
	wg              *sync.WaitGroup
}

func (w *orderNodeMessageWorker) run() {
	defer func() {
		w.wg.Done()
	}()
	for {
		select {
		case msg, ok := <-w.In:
			if !ok {
				close(w.Out)
				return
			}
			msg = w.handler(w.ctx, msg)
			w.Out <- msg
		case <-w.quickExitSignal:
			return
		}
	}
}
