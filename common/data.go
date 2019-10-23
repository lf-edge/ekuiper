package common

import (
	"errors"
	"time"
)

type Rule struct {
	Name, Json string
}

/**** Timer Mock *******/

/** Ticker **/
type Ticker interface {
	GetC() <-chan time.Time
	Stop()
	Trigger(ti int64)
}

type DefaultTicker struct{
	time.Ticker
}

func NewDefaultTicker(d int) *DefaultTicker{
	return &DefaultTicker{*(time.NewTicker(time.Duration(d) * time.Millisecond))}
}

func (t *DefaultTicker) GetC() <-chan time.Time{
	return t.C
}

func (t *DefaultTicker) Trigger(ti int64) {
	Log.Fatal("ticker trigger unsupported")
}

type MockTicker struct {
	c chan time.Time
	duration int64
	lastTick int64
}

func NewMockTicker(d int) *MockTicker{
	if d <= 0 {
		panic(errors.New("non-positive interval for MockTicker"))
	}
	c := make(chan time.Time, 1)
	t := &MockTicker{
		c: c,
		duration: int64(d),
		lastTick: GetMockNow(),
	}
	return t
}

func (t *MockTicker) SetDuration(d int){
	t.duration = int64(d)
	t.lastTick = GetMockNow()
}

func (t *MockTicker) GetC() <-chan time.Time{
	return t.c
}

func (t *MockTicker) Stop() {
	//do nothing
}

func (t *MockTicker) Trigger(ti int64) {
	t.lastTick = ti
	t.c <- time.Unix(ti/1000, ti%1000*1e6)
}

func (t *MockTicker) DoTick(c int64) {
	Log.Infof("do tick at %d, last tick %d", c, t.lastTick)
	if t.lastTick == 0 {
		t.lastTick = c
	}
	if c >= (t.lastTick + t.duration){
		Log.Info("trigger tick")
		t.Trigger(t.lastTick + t.duration)
	}
}

/** Timer **/
type Timer interface {
	GetC() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
	Trigger(ti int64)
}

type DefaultTimer struct{
	time.Timer
}

func NewDefaultTimer(d int) *DefaultTimer{
	return &DefaultTimer{*(time.NewTimer(time.Duration(d) * time.Millisecond))}
}

func (t *DefaultTimer) GetC() <-chan time.Time{
	return t.C
}

func (t *DefaultTimer) Trigger(ti int64) {
	Log.Fatal("timer trigger unsupported")
}

type MockTimer struct {
	c chan time.Time
	duration int64
	createdAt int64
}

func NewMockTimer(d int) *MockTimer{
	if d <= 0 {
		panic(errors.New("non-positive interval for MockTimer"))
	}
	c := make(chan time.Time, 1)
	t := &MockTimer{
		c: c,
		duration: int64(d),
		createdAt: GetMockNow(),
	}
	return t
}

func (t *MockTimer) GetC() <-chan time.Time{
	return t.c
}

func (t *MockTimer) Stop() bool{
	t.createdAt = 0
	return true
}

func (t *MockTimer) SetDuration(d int){
	t.duration = int64(d)
	t.createdAt = GetMockNow()
	Log.Infoln("reset timer created at %v", t.createdAt)
}

func (t *MockTimer) Reset(d time.Duration) bool{
	Log.Infoln("reset timer")
	t.SetDuration(int(d.Nanoseconds()/1e6))
	return true
}

func (t *MockTimer) Trigger(ti int64) {
	t.c <- time.Unix(ti/1000, ti%1000*1e6)
	t.createdAt = 0
}

func (t *MockTimer) DoTick(c int64) {
	Log.Infof("do tick at %d, created at", c, t.createdAt)
	if t.createdAt > 0 && c >= (t.createdAt + t.duration){
		Log.Info("trigger timer")
		t.Trigger(t.createdAt + t.duration)
	}
}
