package nexmark

import (
	"math/rand"
	"time"
)

var DefaultEventGenerator *EventGenerator

func init() {
	DefaultEventGenerator = NewEventGenerator()
}

type EventGenerator struct {
	startTS uint64
	eventID int64
	r       *rand.Rand
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

type GenResult struct {
	PersonList   []Person
	AuctionList  []Auction
	BidList      []Bid
	AllEventList []interface{}
}

func NewEventGenerator(opts ...WithGenOption) *EventGenerator {
	g := &EventGenerator{
		startTS: uint64(time.Now().UnixMilli()),
		r:       rand.New(rand.NewSource(int64(rand.Int()))),
	}
	for _, opt := range opts {
		opt(&g.GenOption)
	}
	return g
}

func (g *EventGenerator) inc() {
	g.eventID++
	g.startTS++
}

func (g *EventGenerator) randGen() interface{} {
	g.inc()
	switch g.r.Intn(3) {
	case 0:
		return NewPerson(g.eventID, g.startTS)
	case 1:
		return NewAuction(g.eventID, g.startTS)
	case 2:
		return NewBid(g.eventID, g.startTS)
	}
	panic("gen panic")
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

func (g *EventGenerator) Gen(count int) GenResult {
	r := GenResult{
		PersonList:   make([]Person, 0),
		AuctionList:  make([]Auction, 0),
		BidList:      make([]Bid, 0),
		AllEventList: make([]interface{}, 0),
	}
	for i := 0; i < count; i++ {
		p := g.genPerson()
		r.PersonList = append(r.PersonList, p)
	}
	for i := 0; i < count; i++ {
		a := g.genAuction()
		r.AuctionList = append(r.AuctionList, a)
	}
	for i := 0; i < count; i++ {
		b := g.genBid()
		r.BidList = append(r.BidList, b)
	}
	for i := 0; i < count; i++ {
		if !g.excludePerson {
			r.AllEventList = append(r.AllEventList, r.PersonList[i])
		}
		if !g.excludeAuction {
			r.AllEventList = append(r.AllEventList, r.AuctionList[i])
		}
		if !g.excludeBid {
			r.AllEventList = append(r.AllEventList, r.BidList[i])
		}
	}
	return r
}
