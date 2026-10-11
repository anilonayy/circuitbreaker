package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anilonayy/circuitbreaker/options"
)

// The benchmarks measure what the breaker adds to a call, so the wrapped
// function does nothing. Run them with
//
//	go test -run '^$' -bench . -benchmem -count=5
//
// The Parallel variants call the same breaker from GOMAXPROCS goroutines; every
// request takes the breaker's lock twice (admission and completion), so they
// show the cost of contention rather than a speed-up.

// newBenchBreaker registers a breaker under a key unique to the benchmark and
// removes it when the benchmark ends. Unless opts say otherwise it never opens,
// however many requests fail.
func newBenchBreaker(b *testing.B, opts ...options.Option) *CircuitBreaker {
	b.Helper()

	key := fmt.Sprintf("%s#%d", b.Name(), breakerSeq.Add(1))
	base := []options.Option{
		options.WithMinimumRequestThreshold(math.MaxInt64),
		options.WithRequestTimeout(time.Hour),
	}

	cb := NewCircuitBreaker(key, append(base, opts...)...)
	b.Cleanup(func() { RemoveCircuitBreaker(key) })

	return cb
}

// newOpenBenchBreaker returns a breaker that has been tripped and stays open
// for openTimeout.
func newOpenBenchBreaker(b *testing.B, openTimeout time.Duration, opts ...options.Option) *CircuitBreaker {
	b.Helper()

	base := []options.Option{
		options.WithMinimumRequestThreshold(1),
		options.WithOpenTimeout(openTimeout),
	}

	cb := newBenchBreaker(b, append(base, opts...)...)

	if err := cb.Execute(context.Background(), fail); !errors.Is(err, errBoom) {
		b.Fatalf("tripping request returned %v, want %v", err, errBoom)
	}

	if got := cb.GetState(); got != StateOpen {
		b.Fatalf("state = %s, want %s", got, StateOpen)
	}

	return cb
}

// newHalfOpenBenchBreaker returns a breaker in half-open with probes slots of
// which one is used up by the request that moved it there.
func newHalfOpenBenchBreaker(b *testing.B, probes int64) *CircuitBreaker {
	b.Helper()

	cb := newOpenBenchBreaker(b, time.Nanosecond, options.WithThresholdRequestInHalfOpen(probes))

	time.Sleep(time.Millisecond) // longer than the open timeout

	if err := cb.Execute(context.Background(), succeed); err != nil {
		b.Fatalf("first probe returned %v", err)
	}

	if got := cb.GetState(); got != StateHalfOpen {
		b.Fatalf("state = %s, want %s", got, StateHalfOpen)
	}

	return cb
}

// benchSequential calls exec in a loop and fails the benchmark if a call does
// not return want.
func benchSequential(b *testing.B, exec executeFunc, fn func(context.Context) error, want error) {
	b.Helper()

	ctx := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		if err := exec(ctx, fn); !errors.Is(err, want) {
			b.Fatalf("returned %v, want %v", err, want)
		}
	}
}

// benchParallel is benchSequential on GOMAXPROCS goroutines.
func benchParallel(b *testing.B, exec executeFunc, fn func(context.Context) error, want error) {
	b.Helper()

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := exec(ctx, fn); !errors.Is(err, want) {
				b.Errorf("returned %v, want %v", err, want)

				return
			}
		}
	})
}

// Closed breaker, successful request: the path every healthy call takes.
func BenchmarkExecuteClosed(b *testing.B) {
	benchSequential(b, newBenchBreaker(b).Execute, succeed, nil)
}

func BenchmarkExecuteClosedParallel(b *testing.B) {
	benchParallel(b, newBenchBreaker(b).Execute, succeed, nil)
}

// Closed breaker, failing request: the same path plus the error predicate.
func BenchmarkExecuteClosedFailure(b *testing.B) {
	benchSequential(b, newBenchBreaker(b).Execute, fail, errBoom)
}

// Open breaker: the request is rejected before fn, the timeout context or the
// statistics are touched. This is the price of a fast fail.
func BenchmarkExecuteOpen(b *testing.B) {
	benchSequential(b, newOpenBenchBreaker(b, time.Hour).Execute, succeed, ErrCircuitBreakerOpen)
}

func BenchmarkExecuteOpenParallel(b *testing.B) {
	benchParallel(b, newOpenBenchBreaker(b, time.Hour).Execute, succeed, ErrCircuitBreakerOpen)
}

// Half-open breaker, admitted probe: the round is too long to ever complete, so
// every request takes a probe slot and is counted as a probe result.
func BenchmarkExecuteHalfOpenProbe(b *testing.B) {
	benchSequential(b, newHalfOpenBenchBreaker(b, math.MaxInt64).Execute, succeed, nil)
}

func BenchmarkExecuteHalfOpenProbeParallel(b *testing.B) {
	benchParallel(b, newHalfOpenBenchBreaker(b, math.MaxInt64).Execute, succeed, nil)
}

// Half-open breaker with all probe slots taken: rejected like in open.
func BenchmarkExecuteHalfOpenRejected(b *testing.B) {
	cb := newOpenBenchBreaker(b, time.Nanosecond, options.WithThresholdRequestInHalfOpen(1))

	time.Sleep(time.Millisecond) // longer than the open timeout

	// One probe takes the only slot and stays in flight for the whole benchmark.
	admitted := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- cb.Execute(context.Background(), func(context.Context) error {
			close(admitted)
			<-release

			return nil
		})
	}()

	<-admitted

	benchSequential(b, cb.Execute, succeed, ErrCircuitBreakerOpen)

	b.StopTimer()
	close(release)

	if err := <-done; err != nil {
		b.Fatalf("parked probe returned %v", err)
	}
}

// ExecuteWithTimeout runs fn in a goroutine of its own; this is what that costs
// compared with BenchmarkExecuteClosed.
func BenchmarkExecuteWithTimeoutClosed(b *testing.B) {
	benchSequential(b, newBenchBreaker(b).ExecuteWithTimeout, succeed, nil)
}

func BenchmarkExecuteWithTimeoutClosedParallel(b *testing.B) {
	benchParallel(b, newBenchBreaker(b).ExecuteWithTimeout, succeed, nil)
}

func BenchmarkExecuteWithTimeoutOpen(b *testing.B) {
	benchSequential(b, newOpenBenchBreaker(b, time.Hour).ExecuteWithTimeout, succeed, ErrCircuitBreakerOpen)
}

// The request complete listener is called in a new goroutine for every request.
// "none" is BenchmarkExecuteClosed again, as the baseline next to it; the state
// change listener only runs on a transition, so registering it costs a request
// nothing.
func BenchmarkExecuteClosedListeners(b *testing.B) {
	var completed atomic.Int64

	onComplete := options.WithRequestCompleteListener(func(bool, time.Duration) { completed.Add(1) })
	onStateChange := options.WithStateChangeListener(func(_, _ State) {})

	variants := []struct {
		name string
		opts []options.Option
	}{
		{"none", nil},
		{"state-change", []options.Option{onStateChange}},
		{"request-complete", []options.Option{onComplete}},
		{"both", []options.Option{onStateChange, onComplete}},
	}

	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			benchSequential(b, newBenchBreaker(b, v.opts...).Execute, succeed, nil)
		})
	}
}

// The read side: what a metrics scraper pays per call.
func BenchmarkGetState(b *testing.B) {
	cb := newBenchBreaker(b)

	b.ReportAllocs()

	for b.Loop() {
		_ = cb.GetState()
	}
}

func BenchmarkGetDetailedStatistics(b *testing.B) {
	cb := newBenchBreaker(b)

	for range 100 {
		_ = cb.Execute(context.Background(), succeed)
	}

	b.ReportAllocs()

	for b.Loop() {
		_ = cb.GetDetailedStatistics()
	}
}
