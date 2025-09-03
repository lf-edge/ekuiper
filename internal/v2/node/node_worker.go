package node

import (
	"context"
)

type OrderNodeMessageHandler struct {
	In        chan *NodeMessage
	Out       chan *NodeMessage
	ctx       context.Context
	workers   []*orderNodeMessageWorker
	sendIndex int
	recvIndex int
}

func NewOrderNodeMessageHandler(ctx context.Context, concurrency int, workerHandler func(ctx context.Context, data *NodeMessage) *NodeMessage) *OrderNodeMessageHandler {
	handler := &OrderNodeMessageHandler{
		In:      make(chan *NodeMessage, 16),
		Out:     make(chan *NodeMessage, 16),
		ctx:     ctx,
		workers: make([]*orderNodeMessageWorker, 0),
	}
	for i := 0; i < concurrency; i++ {
		worker := &orderNodeMessageWorker{
			In:      make(chan *NodeMessage, 16),
			Out:     make(chan *NodeMessage, 16),
			ctx:     ctx,
			handler: workerHandler,
		}
		go worker.run()
		handler.workers = append(handler.workers, worker)
	}
	go handler.run()
	return handler
}

func (p *OrderNodeMessageHandler) run() {
	for {
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
		case <-p.ctx.Done():
			return
		}
	}
}

type orderNodeMessageWorker struct {
	In      chan *NodeMessage
	Out     chan *NodeMessage
	ctx     context.Context
	handler func(ctx context.Context, data *NodeMessage) *NodeMessage
}

func (w *orderNodeMessageWorker) run() {
	for {
		select {
		case msg := <-w.In:
			if msg.Control != nil {
				w.Out <- msg
				continue
			}
			msg = w.handler(w.ctx, msg)
			w.Out <- msg
		case <-w.ctx.Done():
			return
		}
	}
}
