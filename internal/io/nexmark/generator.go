package nexmark

import (
	"context"
	"math/rand"
	"time"

	"golang.org/x/time/rate"
)

type EventGenerator struct {
	ctx        context.Context
	cancel     context.CancelFunc
	eventChan  chan map[string]any
	bufferSize int
	qps        int
	startTS    uint64
	eventID    int64
	r          *rand.Rand
	GenOption
}

type GenOption struct {
	excludePerson  bool
	excludeAuction bool
	excludeBid     bool
}

type WithGenOption func(clientConf *GenOption)

func WithExcludePerson() WithGenOption {
	return func(opt *GenOption) {
		opt.excludePerson = true
	}
}

func WithExcludeBid() WithGenOption {
	return func(opt *GenOption) {
		opt.excludeBid = true
	}
}

func WithExcludeAuction() WithGenOption {
	return func(opt *GenOption) {
		opt.excludeAuction = true
	}
}

func NewEventGenerator(parCtx context.Context, qps, bufferSize int, opts ...WithGenOption) *EventGenerator {
	ctx, cancel := context.WithCancel(parCtx)
	g := &EventGenerator{
		ctx:        ctx,
		cancel:     cancel,
		startTS:    uint64(time.Now().UnixMilli()),
		r:          rand.New(rand.NewSource(int64(rand.Int()))),
		bufferSize: bufferSize,
		qps:        qps,
	}
	eventChan := make(chan map[string]any, bufferSize)
	g.eventChan = eventChan
	for _, opt := range opts {
		opt(&g.GenOption)
	}
	return g
}

func (g *EventGenerator) inc() {
	g.eventID++
	g.startTS++
}

func (g *EventGenerator) genPerson() Person {
	g.inc()
	return NewPerson(g.eventID, g.startTS)
}

func (g *EventGenerator) genAuction() Auction {
	g.inc()
	return NewAuction(g.eventID, g.startTS)
}

func (g *EventGenerator) genBid() Bid {
	g.inc()
	return NewBid(g.eventID, g.startTS)
}

func (g *EventGenerator) GenStream() {
	if g.qps <= 0 {
		return
	}
	ctx := g.ctx
	limiter := rate.NewLimiter(rate.Limit(g.qps), 1)
	go func() {
		for {
			if !g.excludePerson {
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				select {
				case g.eventChan <- g.genPerson().ToMap():
				case <-ctx.Done():
					return
				}
			}

			if !g.excludeAuction {
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				select {
				case g.eventChan <- g.genAuction().ToMap():
				case <-ctx.Done():
					return
				}
			}

			if !g.excludeBid {
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				select {
				case g.eventChan <- g.genBid().ToMap():
				case <-ctx.Done():
					return
				}
			}
		}
	}()
}

func (g *EventGenerator) Close() {
	g.cancel()
}
