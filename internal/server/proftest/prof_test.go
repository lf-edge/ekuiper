package proftest

import (
	"context"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/Rookiecom/cpuprofile"

	"github.com/lf-edge/ekuiper/v2/internal/server"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
)

func init() {
	testx.InitEnv("proftest")
}

type testProfile struct{}

func (test *testProfile) StartCPUProfiler(ctx context.Context, t time.Duration) error {
	return nil
}

func (test *testProfile) EnableWindowAggregator(window int) {
	// do nothing
}

func (test *testProfile) GetWindowData() cpuprofile.DataSetAggregateMap {
	return cpuprofile.DataSetAggregateMap{}
}

func (test *testProfile) RegisterTag(tag string, ch chan *cpuprofile.DataSetAggregate) {
	// do nothing
}

type localEkuiperProfile struct{}

func (e *localEkuiperProfile) StartCPUProfiler(ctx context.Context, t time.Duration) error {
	return cpuprofile.StartCPUProfiler(ctx, t)
}

func (e *localEkuiperProfile) EnableWindowAggregator(window int) {
	cpuprofile.EnableWindowAggregator(window)
}

func (e *localEkuiperProfile) GetWindowData() cpuprofile.DataSetAggregateMap {
	return cpuprofile.GetWindowData()
}

func (e *localEkuiperProfile) RegisterTag(tag string, receiveChan chan *cpuprofile.DataSetAggregate) {
	cpuprofile.RegisterTag(tag, receiveChan)
}

func TestStartCPUProfiling(t *testing.T) {
	if testx.Race {
		t.Skip("skip cpu profiling test in race mode")
	}
	ctx, cancel := context.WithCancel(context.Background())

	ekuiperProfiler := &localEkuiperProfile{}
	if err := ekuiperProfiler.StartCPUProfiler(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	ekuiperProfiler.EnableWindowAggregator(5)
	if windowData := ekuiperProfiler.GetWindowData(); windowData == nil {
		t.Fatal("cpu profiling windowData is nil")
	}
	go func(ctx context.Context) {
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, pprof.Labels("rule", "test"))
		pprof.SetGoroutineLabels(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Simulate some work
				for i := 0; i < 1000; i++ {
					_ = i * i
				}
			}
		}
	}(ctx)
	recvCh := make(chan *cpuprofile.DataSetAggregate)
	ekuiperProfiler.RegisterTag("rule", recvCh)
	select {
	case recvData := <-recvCh:
		if recvData == nil {
			t.Fatal("cpu profiling recvData is nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for cpu profiling recvData")
	}

	profiler := &testProfile{}
	err := server.StartCPUProfiling(ctx, profiler, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	cancel()
}
