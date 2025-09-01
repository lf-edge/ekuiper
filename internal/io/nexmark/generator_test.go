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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventGenerator_GenStream(t *testing.T) {
	ctx := context.Background()
	generator := NewEventGenerator(ctx, 10, 100)
	defer generator.Close()

	generator.GenStream()

	var events []map[string]any
	startTime := time.Now()
	timeout := time.After(2 * time.Second)

	for {
		select {
		case event, ok := <-generator.eventChan:
			if !ok {
				goto done
			}
			events = append(events, event)
		case <-timeout:
			goto done
		}
	}

done:
	duration := time.Since(startTime)
	actualQPS := float64(len(events)) / duration.Seconds()

	expectedQPS := 10.0
	tolerance := 0.3
	minExpected := expectedQPS * (1 - tolerance)
	maxExpected := expectedQPS * (1 + tolerance)

	require.GreaterOrEqual(t, actualQPS, minExpected, "QPS should be greater than or equal to minimum expected")
	require.LessOrEqual(t, actualQPS, maxExpected, "QPS should be less than or equal to maximum expected")
	require.NotEmpty(t, events, "Events should be generated")

	personCount := 0
	auctionCount := 0
	bidCount := 0

	for _, event := range events {
		if _, hasID := event["id"]; hasID {
			if _, hasName := event["name"]; hasName {
				personCount++
			} else if _, hasItemName := event["itemName"]; hasItemName {
				auctionCount++
			}
		} else if _, hasAuction := event["auction"]; hasAuction {
			bidCount++
		}
	}

	require.Greater(t, personCount, 0, "Should generate Person events")
	require.Greater(t, auctionCount, 0, "Should generate Auction events")
	require.Greater(t, bidCount, 0, "Should generate Bid events")
}

func TestEventGenerator_GenStreamWithExclusions(t *testing.T) {
	ctx := context.Background()
	generator := NewEventGenerator(ctx, 5, 50, WithExcludePerson())
	defer generator.Close()

	generator.GenStream()

	var events []map[string]any
	timeout := time.After(2 * time.Second)

	for {
		select {
		case event, ok := <-generator.eventChan:
			if !ok {
				goto done
			}
			events = append(events, event)
		case <-timeout:
			goto done
		}
	}

done:
	require.NotEmpty(t, events, "Events should be generated")

	personCount := 0
	auctionCount := 0
	bidCount := 0

	for _, event := range events {
		if _, hasID := event["id"]; hasID {
			if _, hasName := event["name"]; hasName {
				personCount++
			} else if _, hasItemName := event["itemName"]; hasItemName {
				auctionCount++
			}
		} else if _, hasAuction := event["auction"]; hasAuction {
			bidCount++
		}
	}

	require.Equal(t, 0, personCount, "Should not generate Person events when excluded")
	require.Greater(t, auctionCount, 0, "Should generate Auction events")
	require.Greater(t, bidCount, 0, "Should generate Bid events")
}

func TestEventGenerator_GenStreamWithZeroQPS(t *testing.T) {
	ctx := context.Background()
	generator := NewEventGenerator(ctx, 0, 10)
	defer generator.Close()

	generator.GenStream()

	timeout := time.After(200 * time.Millisecond)

	select {
	case event, ok := <-generator.eventChan:
		require.False(t, ok, "No events should be generated with QPS=0, but got: %v", event)
	case <-timeout:
	}
}

func TestEventGenerator_Close(t *testing.T) {
	ctx := context.Background()
	generator := NewEventGenerator(ctx, 10, 100)

	generator.GenStream()

	var events []map[string]any
	timeout := time.After(1 * time.Second)

	for {
		select {
		case event, ok := <-generator.eventChan:
			if !ok {
				goto done
			}
			events = append(events, event)
			if len(events) >= 5 {
				generator.Close()
			}
		case <-timeout:
			goto done
		}
	}

done:
	require.Greater(t, len(events), 0, "Should generate some events before close")
}

func TestEventGenerator_HighQPS(t *testing.T) {
	ctx := context.Background()
	generator := NewEventGenerator(ctx, 100, 1000)
	defer generator.Close()

	generator.GenStream()

	var events []map[string]any
	startTime := time.Now()
	timeout := time.After(1 * time.Second)

	for {
		select {
		case event, ok := <-generator.eventChan:
			if !ok {
				goto done
			}
			events = append(events, event)
		case <-timeout:
			goto done
		}
	}

done:
	duration := time.Since(startTime)
	actualQPS := float64(len(events)) / duration.Seconds()

	expectedQPS := 100.0
	tolerance := 0.3
	minExpected := expectedQPS * (1 - tolerance)
	maxExpected := expectedQPS * (1 + tolerance)

	require.GreaterOrEqual(t, actualQPS, minExpected, "High QPS should be greater than or equal to minimum expected")
	require.LessOrEqual(t, actualQPS, maxExpected, "High QPS should be less than or equal to maximum expected")
	require.NotEmpty(t, events, "Events should be generated")
}
