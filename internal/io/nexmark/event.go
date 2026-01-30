// Copyright 2025 EMQ Technologies Co., Ltd.
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

package nexmark

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
)

var (
	states          []string
	cities          []string
	firstNames      []string
	lastNames       []string
	hotChannels     []string
	PersonIDs       []uint64
	AuctionIDs      []uint64
	categoriesCount int
	mu              sync.RWMutex
)

func init() {
	states = strings.Split("az,ca,id,or,wa,wy", ",")
	cities = strings.Split("phoenix,los angeles,san francisco,boise,portland,bend,redmond,seattle,kent,cheyenne", ",")
	firstNames = strings.Split("peter,paul,luke,john,saul,vicky,kate,julie,sarah,deiter,walter", ",")
	lastNames = strings.Split("shultz,abrams,spencer,white,bartels,walton,smith,jones,noris", ",")
	hotChannels = strings.Split("Google,Facebook,Baidu,Apple", ",")
	PersonIDs = make([]uint64, 0)
	AuctionIDs = make([]uint64, 0)
	categoriesCount = 5
}

type Person struct {
	ID           uint64 `json:"id"`
	Name         string `json:"name"`
	EmailAddress string `json:"emailAddress"`
	CreditCard   string `json:"creditCard"`
	City         string `json:"city"`
	State        string `json:"state"`
	Datetime     uint64 `json:"datetime"`
	Extra        string `json:"extra"`
}

func (p Person) ToMap() map[string]interface{} {
	m := make(map[string]interface{}, 8)
	m["id"] = p.ID
	m["name"] = p.Name
	m["emailAddress"] = p.EmailAddress
	m["creditCard"] = p.CreditCard
	m["city"] = p.City
	m["state"] = p.State
	m["datetime"] = p.Datetime
	m["extra"] = p.Extra
	return m
}

func genPersonID(r *rand.Rand) uint64 {
	id := uint64(r.Int())
	mu.Lock()
	defer mu.Unlock()
	PersonIDs = append(PersonIDs, id)
	return id
}

func pickPersonID(r *rand.Rand) uint64 {
	mu.RLock()
	defer mu.RUnlock()
	return PersonIDs[r.Int()%len(PersonIDs)]
}

func NewPerson(eventID int64, time uint64) Person {
	r := rand.New(rand.NewSource(eventID))
	seed := r.Int()
	name := fmt.Sprintf("%s %s", firstNames[seed%len(firstNames)], lastNames[seed%len(lastNames)])
	emailAddress := fmt.Sprintf("%s@%s.com", randString(7), randString(5))
	creditCard := fmt.Sprintf("%s %s %s %s", randNumber(4), randNumber(4), randNumber(4), randNumber(4))
	city := cities[seed%len(cities)]
	state := states[seed%len(states)]
	extra := randString(r.Intn(20) + 10)
	return Person{
		ID:           genPersonID(r),
		Name:         name,
		EmailAddress: emailAddress,
		CreditCard:   creditCard,
		City:         city,
		State:        state,
		Datetime:     time,
		Extra:        extra,
	}
}

type Auction struct {
	ID          uint64 `json:"id"`
	ItemName    string `json:"itemName"`
	Description string `json:"description"`
	InitialBid  uint64 `json:"initialBid"`
	Reserve     uint64 `json:"reserve"`
	Datetime    uint64 `json:"datetime"`
	Expires     uint64 `json:"expires"`
	Seller      uint64 `json:"seller"`
	Category    uint64 `json:"category"`
	Extra       string `json:"extra"`
}

func (a Auction) ToMap() map[string]interface{} {
	m := make(map[string]interface{}, 10)
	m["id"] = a.ID
	m["itemName"] = a.ItemName
	m["description"] = a.Description
	m["initialBid"] = a.InitialBid
	m["reserve"] = a.Reserve
	m["datetime"] = a.Datetime
	m["expires"] = a.Expires
	m["seller"] = a.Seller
	m["category"] = a.Category
	m["extra"] = a.Extra
	return m
}

func NewAuction(eventID int64, time uint64) Auction {
	r := rand.New(rand.NewSource(eventID))
	itemName := randString(20)
	description := randString(100)
	initialBid := r.Intn(10000)
	reverse := initialBid + r.Intn(10000)
	expires := time + r.Uint64()
	seller := pickPersonID(r)
	category := r.Intn(categoriesCount)
	currentSize := 8 + len(itemName) + len(description) + 8 + 8 + 8 + 8 + 8
	extra := randString(currentSize)
	return Auction{
		ID:          genAuctionID(r),
		ItemName:    itemName,
		Description: description,
		InitialBid:  uint64(initialBid),
		Reserve:     uint64(reverse),
		Datetime:    time,
		Expires:     expires,
		Seller:      seller,
		Category:    uint64(category),
		Extra:       extra,
	}
}

func genAuctionID(r *rand.Rand) uint64 {
	id := uint64(r.Int())
	mu.Lock()
	defer mu.Unlock()
	AuctionIDs = append(AuctionIDs, id)
	return id
}

func pickAuctionID(r *rand.Rand) uint64 {
	mu.RLock()
	defer mu.RUnlock()
	return AuctionIDs[r.Int()%len(AuctionIDs)]
}

type Bid struct {
	Auction  uint64 `json:"auction"`
	Bidder   uint64 `json:"bidder"`
	Price    uint64 `json:"price"`
	Channel  string `json:"channel"`
	Url      string `json:"url"`
	Datetime uint64 `json:"datetime"`
	Extra    string `json:"extra"`
}

func (b Bid) ToMap() map[string]interface{} {
	m := make(map[string]interface{}, 7)
	m["auction"] = b.Auction
	m["bidder"] = b.Bidder
	m["price"] = b.Price
	m["channel"] = b.Channel
	m["url"] = b.Url
	m["datetime"] = b.Datetime
	m["extra"] = b.Extra
	return m
}

func NewBid(eventID int64, time uint64) Bid {
	r := rand.New(rand.NewSource(eventID))
	auction := pickAuctionID(r)
	bidder := pickPersonID(r)
	price := r.Intn(1000)
	channel := hotChannels[r.Int()%len(hotChannels)]
	url := randUrl()
	currentSize := 32
	extra := randString(currentSize)
	return Bid{
		Auction:  auction,
		Bidder:   bidder,
		Price:    uint64(price),
		Channel:  channel,
		Url:      url,
		Datetime: time,
		Extra:    extra,
	}
}

func randString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randNumber(length int) string {
	letters := []rune("0123456789")
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randUrl() string {
	return fmt.Sprintf("https://www.nexmark.com/%s/%s/%s/item.htm?query=1", randString(5), randString(5), randString(5))
}
