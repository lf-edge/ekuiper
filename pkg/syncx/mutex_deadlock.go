//go:build deadlock

package syncx

import "github.com/sasha-s/go-deadlock"

type Mutex struct {
	deadlock.Mutex
}

type RWMutex struct {
	deadlock.RWMutex
}
