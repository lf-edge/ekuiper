// Copyright 2024-2026 EMQ Technologies Co., Ltd.
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

package node

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestSCNLC(t *testing.T) {
	mc := mockclock.GetMockClock()
	expects := []any{
		&xsql.RawTuple{
			Rawdata:   []byte("hello"),
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now(),
			Emitter:   "mock_connector",
		},
		&xsql.RawTuple{
			Emitter:   "mock_connector",
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now(),
		},
		&xsql.RawTuple{
			Rawdata:   []byte("world"),
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now(),
			Emitter:   "mock_connector",
		},
	}
	var sc api.BytesSource = &MockSourceConnector{
		data: [][]byte{
			[]byte("hello"),
			nil,
			[]byte("world"),
		},
	}
	ctx := mockContext.NewMockContext("rule1", "src1")
	errCh := make(chan error)
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	result := make(chan any, 10)
	err = scn.AddOutput(result, "testResult")
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	limit := len(expects)
	actual := make([]any, 0, limit)
	go func() {
		defer wg.Done()
		ticker := time.After(2000 * time.Second)
		for {
			select {
			case sg := <-errCh:
				switch et := sg.(type) {
				case error:
					assert.Fail(t, et.Error())
					return
				default:
					fmt.Println("ctrlCh", et)
				}
			case tuple := <-result:
				actual = append(actual, tuple)
				limit--
				if limit <= 0 {
					return
				}
			case <-ticker:
				assert.Fail(t, "timeout")
				return
			}
		}
	}()
	scn.Open(ctx, errCh)
	wg.Wait()
	for i := 0; i < len(expects); i++ {
		exp := expects[i].(*xsql.RawTuple)
		got := actual[i].(*xsql.RawTuple)
		require.Equal(t, exp.Rawdata, got.Rawdata)
		require.Equal(t, exp.Metadata, got.Metadata)
		require.Equal(t, exp.Emitter, got.Emitter)
	}
}

func TestNewError(t *testing.T) {
	var sc api.BytesSource = &MockSourceConnector{
		data: [][]byte{
			[]byte("hello"),
			[]byte("world"),
		},
	}
	ctx := mockContext.NewMockContext("rule1", "src1")
	_, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.Error(t, err)
	assert.Equal(t, "datasource name cannot be empty", err.Error())
	_, err = NewSourceNode(ctx, "mock_connector", sc, map[string]any{"interval": "invalid", "datasource": "demo"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.Error(t, err)
	assert.Equal(t, "1 error(s) decoding:\n\n* error decoding 'interval': time: invalid duration \"invalid\"", err.Error())

	var pc api.PullTupleSource = &MockPullSource{}
	_, err = NewSourceNode(ctx, "mock_connector", pc, map[string]any{"datasource": "demo", "interval": "1s"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	assert.True(t, pc.(*MockPullSource).set)
}

func TestConnError(t *testing.T) {
	var sc api.BytesSource = &MockSourceConnector{
		data: nil, // nil data to produce mock connect error
	}
	ctx := mockContext.NewMockContext("rule1", "src1")
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo2"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	var errResult error
	go func() {
		defer wg.Done()
		ticker := time.After(2 * time.Second)
		for {
			select {
			case sg := <-errCh:
				switch et := sg.(type) {
				case error:
					errResult = et
					return
				default:
					fmt.Println("ctrlCh", et)
				}
			case <-ticker:
				return
			}
		}
	}()
	scn.Open(ctx, errCh)
	wg.Wait()
	assert.Error(t, errResult)
	assert.Equal(t, "data is nil", errResult.Error())
}

func TestPull(t *testing.T) {
	mc := mockclock.GetMockClock()
	expects := []any{
		&xsql.Tuple{
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now(),
			Emitter:   "mock_connector",
			Message:   map[string]any{"index": 1},
		},
		&xsql.RawTuple{
			Emitter:   "mock_connector",
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now().Add(time.Second),
			Rawdata:   []byte{2},
		},
		&xsql.Tuple{
			Timestamp: mc.Now().Add(2 * time.Second),
			Emitter:   "mock_connector",
			Message:   map[string]any{"index": 3},
		},
		&xsql.Tuple{
			Timestamp: mc.Now().Add(3 * time.Second),
			Emitter:   "mock_connector",
			Message:   map[string]any{"index": 4},
		},
		&xsql.Tuple{
			Timestamp: mc.Now().Add(4 * time.Second),
			Emitter:   "mock_connector",
			Message:   map[string]any{"index": 5},
			Metadata:  map[string]any{"topic": "demo"},
		},
	}
	var sc api.PullTupleSource = &MockPullSource{}
	ctx := mockContext.NewMockContext("rule1", "src1")
	errCh := make(chan error)
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo", "interval": "1s"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	result := make(chan any, 10)
	err = scn.AddOutput(result, "testResult")
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	limit := len(expects)
	actual := make([]any, 0, limit)
	go func() {
		defer wg.Done()
		ticker := time.After(2000 * time.Second)
		for {
			select {
			case sg := <-errCh:
				switch et := sg.(type) {
				case error:
					assert.Fail(t, et.Error())
					return
				default:
					fmt.Println("ctrlCh", et)
				}
			case tuple := <-result:
				actual = append(actual, tuple)
				limit--
				if limit <= 0 {
					return
				}
			case <-ticker:
				assert.Fail(t, "timeout")
				return
			}
		}
	}()
	scn.Open(ctx, errCh)
	time.Sleep(10 * time.Millisecond)
	timex.Add(10 * time.Second)
	wg.Wait()
	assert.Equal(t, expects, actual)
}

type MockSourceConnector struct {
	data       [][]byte
	topic      string
	subscribed atomic.Bool
}

func (m *MockSourceConnector) Provision(ctx api.StreamContext, configs map[string]any) error {
	datasource, ok := configs["datasource"]
	if !ok {
		return fmt.Errorf("datasource name cannot be empty")
	}
	m.topic = datasource.(string)
	return nil
}

func (m *MockSourceConnector) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	if m.data == nil {
		return fmt.Errorf("data is nil")
	}
	return nil
}

func (m *MockSourceConnector) Close(ctx api.StreamContext) error {
	if m.subscribed.Load() {
		m.subscribed.Store(false)
		return nil
	} else {
		return fmt.Errorf("not subscribed")
	}
}

func (m *MockSourceConnector) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	if m.subscribed.Load() {
		return fmt.Errorf("already subscribed")
	}
	m.subscribed.Store(true)
	go func() {
		if !m.subscribed.Load() {
			time.Sleep(100 * time.Millisecond)
		}
		for _, d := range m.data {
			ingest(ctx, d, map[string]any{"topic": "demo"}, timex.GetNow())
		}
		<-ctx.Done()
		fmt.Println("MockSourceConnector closed")
	}()
	return nil
}

type MockPullSource struct {
	set       bool
	pullTimes int
}

func (m *MockPullSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *MockPullSource) Close(ctx api.StreamContext) error {
	return nil
}

func (m *MockPullSource) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *MockPullSource) Pull(ctx api.StreamContext, trigger time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	m.pullTimes++
	var mess any
	switch m.pullTimes % 5 {
	case 0:
		mess = map[string]any{
			"index": m.pullTimes,
		}
	case 1:
		mess = []map[string]any{
			{
				"index": m.pullTimes,
			},
		}
	case 2:
		mess = []byte{byte(m.pullTimes)}
	case 3:
		mess = &xsql.Tuple{
			Message: map[string]any{
				"index": m.pullTimes,
			},
		}
	case 4:
		mess = []*xsql.Tuple{
			{
				Message: map[string]any{
					"index": m.pullTimes,
				},
			},
		}
	}
	ingest(ctx, mess, map[string]any{"topic": "demo"}, trigger)
}

func (m *MockPullSource) SetEofIngest(eof api.EOFIngest) {
	m.set = true
}

type MockRewindSource struct {
	notify   chan struct{}
	ingested chan struct{}
	state    int
}

func (m *MockRewindSource) GetOffset() (any, error) {
	return m.state, nil
}

func (m *MockRewindSource) Rewind(offset any) error {
	m.state = offset.(int)
	return nil
}

func (m *MockRewindSource) ResetOffset(input map[string]any) error {
	return nil
}

func (m *MockRewindSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	return nil
}

func (m *MockRewindSource) Close(ctx api.StreamContext) error {
	return nil
}

func (m *MockRewindSource) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	return nil
}

func (m *MockRewindSource) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) error {
	go func() {
		for range m.notify {
			current := m.state
			m.state++
			ingest(ctx, map[string]any{
				"key": current,
			}, nil, time.Now())
			m.ingested <- struct{}{}
		}
	}()
	return nil
}

func TestMockRewind(t *testing.T) {
	notify := make(chan struct{})
	m := &MockRewindSource{
		notify:   notify,
		ingested: make(chan struct{}),
	}
	var sc api.TupleSource = m
	ctx := mockContext.NewMockContext("rule1", "src1")
	// set rewind value
	ctx.PutState(OffsetKey, 10)
	errCh := make(chan error)
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	result := make(chan any, 10)
	err = scn.AddOutput(result, "testResult")
	assert.NoError(t, err)
	scn.Open(ctx, errCh)
	notify <- struct{}{}
	data := <-result
	require.Equal(t, map[string]interface{}{"key": 10}, map[string]interface{}(data.(*xsql.Tuple).Message))
	<-m.ingested
	notify <- struct{}{}
	data = <-result
	require.Equal(t, map[string]interface{}{"key": 11}, map[string]interface{}(data.(*xsql.Tuple).Message))
	<-m.ingested
	v, _ := ctx.GetState(OffsetKey)
	require.Equal(t, 12, v)
}

func TestSourceOffsetIsOwnedByContext(t *testing.T) {
	offset := map[string]any{
		"partition": map[string]any{"position": 10},
	}
	source := &MutableRewindSource{MockRewindSource: MockRewindSource{}, offset: offset}
	node := &SourceNode{s: source}
	ctx := mockContext.NewMockContext("rule1", "src1")

	require.NoError(t, node.updateState(ctx))
	offset["partition"].(map[string]any)["position"] = 20

	saved, err := ctx.GetState(OffsetKey)
	require.NoError(t, err)
	require.Equal(t, 10, saved.(map[string]any)["partition"].(map[string]any)["position"])
}

func TestImmutableSourceOffsetUsesDirectOwnershipTransfer(t *testing.T) {
	type immutableOffset struct {
		position int
	}
	offset := &immutableOffset{position: 10}
	source := &ImmutableRewindSource{offset: offset}
	node := &SourceNode{s: source}
	ctx := mockContext.NewMockContext("rule1", "src1")

	require.NoError(t, node.updateState(ctx))
	saved, err := ctx.GetState(OffsetKey)
	require.NoError(t, err)
	require.Same(t, offset, saved)
}

func BenchmarkSourceOffsetUpdate(b *testing.B) {
	for _, size := range []int{1, 10_000} {
		offset := make(map[string]any, size)
		for i := range size {
			offset[fmt.Sprintf("partition-%d", i)] = int64(i)
		}
		sources := map[string]api.Rewindable{
			"Mutable":   &MutableRewindSource{offset: offset},
			"Immutable": &ImmutableRewindSource{offset: offset},
		}
		for name, source := range sources {
			b.Run(fmt.Sprintf("%s/%d", name, size), func(b *testing.B) {
				ctx := mockContext.NewMockContext("rule1", "src1")
				node := &SourceNode{s: source.(api.Source)}
				b.ReportAllocs()
				for b.Loop() {
					if err := node.updateState(ctx); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestSourceCheckpointErrorClearsAfterOffsetRecovery(t *testing.T) {
	offsetErr := errors.New("offset unavailable")
	source := &MutableRewindSource{
		MockRewindSource: MockRewindSource{},
		offset:           map[string]any{"position": 10},
		offsetErr:        offsetErr,
	}
	node := &SourceNode{s: source}
	ctx := mockContext.NewMockContext("rule1", "src1")

	node.checkpointMu.Lock()
	err := node.refreshCheckpointState(ctx)
	require.ErrorIs(t, err, offsetErr)
	require.ErrorIs(t, node.CheckpointError(), offsetErr)
	node.checkpointMu.Unlock()

	source.offsetErr = nil
	node.checkpointMu.Lock()
	require.NoError(t, node.refreshCheckpointState(ctx))
	require.NoError(t, node.CheckpointError())
	node.checkpointMu.Unlock()

	saved, err := ctx.GetState(OffsetKey)
	require.NoError(t, err)
	require.Equal(t, source.offset, saved)
}

func TestSourceCheckpointWaitsForOffsetCaptureAndRecovers(t *testing.T) {
	offsetErr := errors.New("offset unavailable")
	source := &controlledCheckpointSource{offset: 10}
	node, ctx, store, output := newCheckpointSourceNode(t, source)

	node.ingestAnyTuple(ctx, map[string]any{"value": 10}, nil, time.Now())
	require.IsType(t, &xsql.Tuple{}, receiveCheckpointOutput(t, output))

	offsetEntered, offsetRelease := source.blockNextOffset(20, offsetErr)
	var releaseOnce sync.Once
	releaseOffset := func() {
		releaseOnce.Do(func() {
			close(offsetRelease)
		})
	}
	defer releaseOffset()
	ingestDone := make(chan struct{})
	go func() {
		defer close(ingestDone)
		node.ingestAnyTuple(ctx, map[string]any{"value": 20}, nil, time.Now())
	}()
	waitCheckpointStep(t, offsetEntered, "GetOffset to block")
	require.IsType(t, &xsql.Tuple{}, receiveCheckpointOutput(t, output))

	signals := make(chan *checkpoint.Signal, 2)
	lockAttempted := make(chan struct{})
	responder := checkpoint.NewResponderExecutor(signals, &checkpointSourceTask{
		SourceNode:    node,
		lockAttempted: lockAttempted,
	})
	triggerDone := make(chan error, 1)
	go func() {
		triggerDone <- responder.TriggerCheckpoint(1)
	}()
	waitCheckpointStep(t, lockAttempted, "checkpoint to attempt the source lock")

	select {
	case err := <-triggerDone:
		t.Fatalf("checkpoint completed while GetOffset was blocked: %v", err)
	default:
	}
	select {
	case item := <-output:
		t.Fatalf("barrier was emitted while GetOffset was blocked: %#v", item)
	default:
	}
	select {
	case signal := <-signals:
		t.Fatalf("checkpoint terminated while GetOffset was blocked: %#v", signal)
	default:
	}

	releaseOffset()
	waitCheckpointStep(t, ingestDone, "source ingest to finish")
	require.ErrorIs(t, receiveCheckpointError(t, triggerDone), offsetErr)
	barrier := receiveCheckpointOutput(t, output)
	require.Equal(t, int64(1), barrier.(*checkpoint.Barrier).CheckpointId)
	signal := receiveCheckpointSignal(t, signals)
	require.Equal(t, checkpoint.DEC, signal.Message)
	require.Equal(t, int64(1), signal.CheckpointId)
	require.Empty(t, signals, "failed source checkpoint must not emit ACK")
	staleOffset, err := ctx.GetState(OffsetKey)
	require.NoError(t, err)
	require.Equal(t, 10, staleOffset)
	_, saved := store.frozen.Load(int64(1))
	require.False(t, saved, "failed source checkpoint must not save the stale offset")

	source.setOffset(30, nil)
	node.ingestAnyTuple(ctx, map[string]any{"value": 30}, nil, time.Now())
	require.IsType(t, &xsql.Tuple{}, receiveCheckpointOutput(t, output))

	recoverySignals := make(chan *checkpoint.Signal, 1)
	recoveryResponder := checkpoint.NewResponderExecutor(recoverySignals, &checkpointSourceTask{
		SourceNode:    node,
		lockAttempted: make(chan struct{}),
	})
	require.NoError(t, recoveryResponder.TriggerCheckpoint(2))
	barrier = receiveCheckpointOutput(t, output)
	require.Equal(t, int64(2), barrier.(*checkpoint.Barrier).CheckpointId)
	signal = receiveCheckpointSignal(t, recoverySignals)
	require.Equal(t, checkpoint.ACK, signal.Message)
	require.Equal(t, int64(2), signal.CheckpointId)

	frozen, saved := store.frozen.Load(int64(2))
	require.True(t, saved, "recovered source checkpoint was not saved")
	snapshot, err := checkpoint.DecodeState(frozen.([]byte))
	require.NoError(t, err)
	require.Equal(t, 30, snapshot[OffsetKey])
}

func TestSourceCheckpointRejectsUnsupportedGobOffset(t *testing.T) {
	source := &controlledCheckpointSource{offset: make(chan int)}
	node, ctx, store, output := newCheckpointSourceNode(t, source)

	node.ingestAnyTuple(ctx, map[string]any{"value": 1}, nil, time.Now())
	require.IsType(t, &xsql.Tuple{}, receiveCheckpointOutput(t, output))

	signals := make(chan *checkpoint.Signal, 2)
	responder := checkpoint.NewResponderExecutor(signals, &checkpointSourceTask{
		SourceNode:    node,
		lockAttempted: make(chan struct{}),
	})
	require.ErrorContains(t, responder.TriggerCheckpoint(3), "gob")
	barrier := receiveCheckpointOutput(t, output)
	require.Equal(t, int64(3), barrier.(*checkpoint.Barrier).CheckpointId)
	signal := receiveCheckpointSignal(t, signals)
	require.Equal(t, checkpoint.DEC, signal.Message)
	require.Equal(t, int64(3), signal.CheckpointId)
	require.Empty(t, signals, "unsupported offset checkpoint must not emit ACK")
	_, saved := store.frozen.Load(int64(3))
	require.False(t, saved, "unsupported offset checkpoint must not save state")
}

func newCheckpointSourceNode(
	t *testing.T,
	source api.Source,
) (*SourceNode, api.StreamContext, *sourceCheckpointCaptureStore, chan any) {
	t.Helper()
	store := &sourceCheckpointCaptureStore{}
	ctx := topoContext.Background().WithMeta("checkpoint_rule", "checkpoint_source", store)
	node, err := NewSourceNode(
		ctx,
		"checkpoint_source",
		source,
		map[string]any{"datasource": "test"},
		&def.RuleOption{BufferLength: 16},
	)
	require.NoError(t, err)
	node.prepareExec(ctx, make(chan error, 1), "source")
	node.SetQos(def.AtLeastOnce)
	output := make(chan any, 16)
	require.NoError(t, node.AddOutput(output, "test"))
	return node, ctx, store, output
}

func receiveCheckpointOutput(t *testing.T, output <-chan any) any {
	t.Helper()
	select {
	case item := <-output:
		if wrapped, ok := item.(*checkpoint.BufferOrEvent); ok {
			return wrapped.Data
		}
		return item
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for source output")
		return nil
	}
}

func receiveCheckpointSignal(t *testing.T, signals <-chan *checkpoint.Signal) *checkpoint.Signal {
	t.Helper()
	select {
	case signal := <-signals:
		return signal
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for checkpoint signal")
		return nil
	}
}

func receiveCheckpointError(t *testing.T, errors <-chan error) error {
	t.Helper()
	select {
	case err := <-errors:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for checkpoint result")
		return nil
	}
}

func waitCheckpointStep(t *testing.T, done <-chan struct{}, step string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", step)
	}
}

type checkpointSourceTask struct {
	*SourceNode
	lockAttempted chan struct{}
}

func (t *checkpointSourceTask) LockCheckpoint() {
	close(t.lockAttempted)
	t.SourceNode.LockCheckpoint()
}

type controlledCheckpointSource struct {
	MockRewindSource

	mu            sync.Mutex
	offset        any
	offsetErr     error
	offsetEntered chan struct{}
	offsetRelease chan struct{}
}

func (s *controlledCheckpointSource) GetOffset() (any, error) {
	s.mu.Lock()
	offset := s.offset
	offsetErr := s.offsetErr
	entered := s.offsetEntered
	release := s.offsetRelease
	s.offsetEntered = nil
	s.offsetRelease = nil
	s.mu.Unlock()
	if entered != nil {
		close(entered)
		<-release
	}
	return offset, offsetErr
}

func (s *controlledCheckpointSource) setOffset(offset any, offsetErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offset = offset
	s.offsetErr = offsetErr
}

func (s *controlledCheckpointSource) blockNextOffset(offset any, offsetErr error) (<-chan struct{}, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offset = offset
	s.offsetErr = offsetErr
	s.offsetEntered = make(chan struct{})
	s.offsetRelease = make(chan struct{})
	return s.offsetEntered, s.offsetRelease
}

type sourceCheckpointCaptureStore struct {
	state.MemoryStore
	frozen sync.Map
}

func (s *sourceCheckpointCaptureStore) SaveFrozenState(checkpointID int64, _ string, frozen []byte) error {
	s.frozen.Store(checkpointID, append([]byte(nil), frozen...))
	return nil
}

type MutableRewindSource struct {
	MockRewindSource
	offset    map[string]any
	offsetErr error
}

func (m *MutableRewindSource) GetOffset() (any, error) {
	return m.offset, m.offsetErr
}

func (m *MutableRewindSource) Rewind(offset any) error {
	m.offset = offset.(map[string]any)
	return nil
}

type ImmutableRewindSource struct {
	MockRewindSource
	offset any
}

func (m *ImmutableRewindSource) GetOffset() (any, error) {
	return m.offset, nil
}

func (m *ImmutableRewindSource) CheckpointOffsetIsImmutable() {}

var _ checkpoint.ImmutableOffsetProvider = (*ImmutableRewindSource)(nil)
