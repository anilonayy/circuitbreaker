package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anilonayy/circuitbreaker/options"
)

// The tests in this file cover the breaker under concurrent use. Anything that
// depends on the open timeout or the request timeout runs inside a synctest
// bubble: time is virtual there and synctest.Wait blocks until every goroutine
// has settled, so the tests neither sleep for real nor guess at scheduling.
//
// Several of them are regression tests: each pins down a bug this package
// shipped with, and its comment starts with "Regression:" followed by what used
// to go wrong. Run them on their own with
//
//	go test -race -run 'Regression' -v .
//
// which works because the bug is also named in the test function:
//
//	TestRegressionUnlockedAdmissionRace             canProceed read and wrote state without the lock
//	TestRegressionOpenToHalfOpenHappensOnce         the same race, seen as duplicate transitions
//	TestRegressionSettersRaceWithExecute            Set* raced with requests in flight
//	TestRegressionHalfOpenOverAdmission             half-open let more than the threshold through
//	TestRegressionHalfOpenStuckOnTimeout            a round of timed out probes never ended
//	TestRegressionStaleRequestCountedAsProbe        a request admitted in CLOSED counted as a probe
//	TestRegressionPanic...                          a panicking request was not recorded at all
//	TestRegressionDetailedStatisticsLeakedPointers  statistics pointed into the live breaker

const testOpenTimeout = time.Second

var (
	errBoom    = errors.New("boom")
	breakerSeq atomic.Int64
)

// newTestBreaker registers a breaker under a key unique to the calling test and
// removes it again when the test ends, so repeated runs (-count) never pick up
// a breaker left behind by an earlier one. A single failure trips it.
func newTestBreaker(t *testing.T, opts ...options.Option) *CircuitBreaker {
	t.Helper()

	key := fmt.Sprintf("%s#%d", t.Name(), breakerSeq.Add(1))
	base := []options.Option{
		options.WithMinimumRequestThreshold(1),
		options.WithFailureRateThreshold(0.5),
		options.WithOpenTimeout(testOpenTimeout),
		options.WithRequestTimeout(time.Hour),
	}

	cb := NewCircuitBreaker(key, append(base, opts...)...)
	t.Cleanup(func() { RemoveCircuitBreaker(key) })

	return cb
}

func succeed(context.Context) error { return nil }

func fail(context.Context) error { return errBoom }

// tripOpen fails one request, which is enough to open a breaker built by
// newTestBreaker.
func tripOpen(t *testing.T, cb *CircuitBreaker) {
	t.Helper()

	if err := cb.Execute(context.Background(), fail); !errors.Is(err, errBoom) {
		t.Fatalf("tripping request returned %v, want %v", err, errBoom)
	}

	requireState(t, cb, StateOpen)
}

func requireState(t *testing.T, cb *CircuitBreaker, want State) {
	t.Helper()

	if got := cb.GetState(); got != want {
		t.Fatalf("state = %s, want %s", got, want)
	}
}

func checkState(t *testing.T, cb *CircuitBreaker, want State) {
	t.Helper()

	if got := cb.GetState(); got != want {
		t.Errorf("state = %s, want %s", got, want)
	}
}

// transitionLog collects state changes reported to a state change listener.
type transitionLog struct {
	mu  sync.Mutex
	got []string
}

func (l *transitionLog) record(from, to State) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.got = append(l.got, from.String()+"->"+to.String())
}

func (l *transitionLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.got)
}

// Regression: canProceed used to read and write the state, the transition time
// and the half-open counters without holding the lock, so concurrent Execute
// calls raced with each other, with the recording path and with every reader.
//
// Many goroutines drive the breaker through every state while others read it.
// The assertions are deliberately loose; the race detector is the real judge.
func TestRegressionUnlockedAdmissionRace(t *testing.T) {
	const (
		workers    = 16
		iterations = 300
		threshold  = 4
	)

	var (
		transitions, completions atomic.Int64
		overAdmitted             atomic.Bool
	)

	cb := newTestBreaker(t,
		// Reopen for probing almost immediately so the breaker keeps cycling
		// through OPEN and HALF_OPEN instead of parking in OPEN.
		options.WithOpenTimeout(time.Nanosecond),
		options.WithMinimumRequestThreshold(5),
		options.WithFailureRateThreshold(0.1),
		options.WithThresholdRequestInHalfOpen(threshold),
		options.WithTargetRateInHalfOpen(0.5),
		options.WithStateChangeListener(func(_, _ State) { transitions.Add(1) }),
		options.WithRequestCompleteListener(func(bool, time.Duration) { completions.Add(1) }),
	)

	ctx := context.Background()
	stop := make(chan struct{})

	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}

				_ = cb.GetState()
				_, _, _, _ = cb.GetStatistics()
				_ = cb.String()

				stats := cb.GetDetailedStatistics()
				if n := stats.HalfOpenRequestCount; n != nil && *n > threshold {
					overAdmitted.Store(true)
				}
				if n := stats.RemainingTestRequests; n != nil && *n < 0 {
					overAdmitted.Store(true)
				}
			}
		})
	}

	var writers sync.WaitGroup
	for w := range workers {
		writers.Go(func() {
			for i := range iterations {
				// Alternate between failing and succeeding stretches so the
				// breaker both opens and recovers.
				var want error
				if (i/25)%2 == 0 {
					want = errBoom
				}

				exec := cb.Execute
				if (w+i)%2 == 1 {
					exec = cb.ExecuteWithTimeout
				}

				err := exec(ctx, func(context.Context) error { return want })
				if !errors.Is(err, want) && !errors.Is(err, ErrCircuitBreakerOpen) {
					t.Errorf("Execute returned %v, want %v or %v", err, want, ErrCircuitBreakerOpen)
				}
			}
		})
	}

	writers.Wait()
	close(stop)
	readers.Wait()

	if overAdmitted.Load() {
		t.Errorf("a reader saw more than %d probes admitted in half-open", threshold)
	}

	t.Logf("state changes observed: %d, completed requests observed: %d", transitions.Load(), completions.Load())
}

// Regression: Execute used to read the request timeout, the error predicate and
// both listeners straight from the breaker while the Set* methods wrote them
// under the lock, a data race for anyone reconfiguring a live breaker.
//
// The public setters may be called while requests are in flight. Like the test
// above this one is decided by the race detector.
func TestRegressionSettersRaceWithExecute(t *testing.T) {
	const (
		workers    = 8
		iterations = 200
	)

	// The breaker stays closed throughout; only the shared configuration is contended.
	cb := newTestBreaker(t, options.WithMinimumRequestThreshold(1_000_000))
	ctx := context.Background()

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range iterations {
				exec := cb.Execute
				if (w+i)%2 == 1 {
					exec = cb.ExecuteWithTimeout
				}

				if err := exec(ctx, fail); !errors.Is(err, errBoom) {
					t.Errorf("Execute returned %v, want %v", err, errBoom)
				}
			}
		})
	}

	wg.Go(func() {
		for range iterations {
			cb.SetRequestTimeout(time.Hour)
			cb.SetOpenTimeout(testOpenTimeout)
			cb.SetTimeWindow(time.Minute)
			cb.SetMinimumRequestThreshold(1_000_000)
			cb.SetFailureRateThreshold(0.5)
			cb.SetThresholdRequestInHalfOpen(3)
			cb.SetTargetRateInHalfOpen(0.5)
			cb.SetCustomErrorPredicate(func(err error) bool { return err != nil })
			cb.SetStateChangeListener(func(_, _ State) {})
			cb.SetRequestCompleteListener(func(bool, time.Duration) {})
		}
	})

	wg.Wait()
	checkState(t, cb, StateClosed)
}

// Regression: half-open used to compare the number of completed probes with the
// threshold, so every caller that arrived before the first probe finished was
// let through: twenty concurrent callers meant twenty probes against a
// dependency that had just been failing, whatever the threshold said.
//
// Half-open must let through at most thresholdRequestInHalfOpen probes, counting
// the ones that are still in flight, and reject everyone else.
func TestRegressionHalfOpenOverAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			threshold = 3
			callers   = 20
		)

		cb := newTestBreaker(t,
			options.WithThresholdRequestInHalfOpen(threshold),
			options.WithTargetRateInHalfOpen(1.0),
		)
		tripOpen(t, cb)
		time.Sleep(testOpenTimeout)

		var admitted, rejected atomic.Int64

		release := make(chan struct{})

		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				err := cb.Execute(context.Background(), func(context.Context) error {
					admitted.Add(1)
					<-release

					return nil
				})

				switch {
				case errors.Is(err, ErrCircuitBreakerOpen):
					rejected.Add(1)
				case err != nil:
					t.Errorf("Execute returned %v", err)
				}
			})
		}

		// Every caller has now either been rejected or is parked inside fn;
		// not a single probe has completed yet.
		synctest.Wait()

		if got := admitted.Load(); got != threshold {
			t.Errorf("half-open admitted %d concurrent probes, want %d", got, threshold)
		}
		if got := rejected.Load(); got != callers-threshold {
			t.Errorf("half-open rejected %d callers, want %d", got, callers-threshold)
		}
		checkState(t, cb, StateHalfOpen)

		stats := cb.GetDetailedStatistics()
		if n := stats.HalfOpenRequestCount; n == nil || *n != threshold {
			t.Errorf("HalfOpenRequestCount = %v, want %d", deref(n), threshold)
		}
		if n := stats.RemainingTestRequests; n == nil || *n != 0 {
			t.Errorf("RemainingTestRequests = %v, want 0", deref(n))
		}

		close(release)
		wg.Wait()

		// All admitted probes succeeded, so the breaker closes.
		checkState(t, cb, StateClosed)
	})
}

// Regression: with canProceed unlocked, each caller that found the open timeout
// expired performed the OPEN -> HALF_OPEN transition itself, resetting the
// probe counters under the others and firing the listener once per caller.
//
// Callers that hit an expired open timeout at the same instant must produce a
// single OPEN -> HALF_OPEN transition and share one set of probe slots.
func TestRegressionOpenToHalfOpenHappensOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			threshold = 2
			callers   = 64
		)

		var log transitionLog

		cb := newTestBreaker(t,
			options.WithThresholdRequestInHalfOpen(threshold),
			options.WithTargetRateInHalfOpen(1.0),
			options.WithStateChangeListener(log.record),
		)
		tripOpen(t, cb)
		time.Sleep(testOpenTimeout)

		var admitted atomic.Int64

		start := make(chan struct{})
		release := make(chan struct{})

		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				<-start

				_ = cb.Execute(context.Background(), func(context.Context) error {
					admitted.Add(1)
					<-release

					return nil
				})
			})
		}

		synctest.Wait() // everyone is lined up on start
		close(start)
		synctest.Wait() // everyone is rejected or parked inside fn, listeners have run

		want := []string{"CLOSED->OPEN", "OPEN->HALF_OPEN"}
		if got := log.snapshot(); !slices.Equal(got, want) {
			t.Errorf("transitions = %v, want %v", got, want)
		}
		if got := admitted.Load(); got != threshold {
			t.Errorf("admitted %d probes across the transition, want %d", got, threshold)
		}

		close(release)
		wg.Wait()
		synctest.Wait()

		want = append(want, "HALF_OPEN->CLOSED")
		if got := log.snapshot(); !slices.Equal(got, want) {
			t.Errorf("transitions = %v, want %v", got, want)
		}
	})
}

// Regression: the timeout paths of Execute and ExecuteWithTimeout returned
// before the half-open evaluation, so a timed out probe was never counted. A
// dependency that hung instead of failing kept the breaker in half-open for
// good: the round could not complete, and it kept admitting probes.
//
// A probe that times out is a failed probe. If every probe times out the
// breaker must reopen instead of sitting in half-open, and it must still be
// able to recover afterwards.
func TestRegressionHalfOpenStuckOnTimeout(t *testing.T) {
	const threshold = 3

	honorsContext := func(ctx context.Context) error {
		<-ctx.Done()

		return ctx.Err()
	}

	tests := []struct {
		name string
		exec func(*CircuitBreaker) func(context.Context, func(context.Context) error) error
		slow func(hang <-chan struct{}) func(context.Context) error
	}{
		{
			name: "Execute",
			exec: func(cb *CircuitBreaker) func(context.Context, func(context.Context) error) error { return cb.Execute },
			slow: func(<-chan struct{}) func(context.Context) error { return honorsContext },
		},
		{
			name: "ExecuteWithTimeout/fn honors context",
			exec: func(cb *CircuitBreaker) func(context.Context, func(context.Context) error) error {
				return cb.ExecuteWithTimeout
			},
			slow: func(<-chan struct{}) func(context.Context) error { return honorsContext },
		},
		{
			name: "ExecuteWithTimeout/fn ignores context",
			exec: func(cb *CircuitBreaker) func(context.Context, func(context.Context) error) error {
				return cb.ExecuteWithTimeout
			},
			slow: func(hang <-chan struct{}) func(context.Context) error {
				return func(context.Context) error {
					<-hang

					return nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cb := newTestBreaker(t,
					options.WithRequestTimeout(100*time.Millisecond),
					options.WithThresholdRequestInHalfOpen(threshold),
					options.WithTargetRateInHalfOpen(0.5),
				)
				exec := tt.exec(cb)
				ctx := context.Background()

				hang := make(chan struct{})
				defer close(hang) // lets the abandoned fn goroutines finish

				tripOpen(t, cb)
				time.Sleep(testOpenTimeout)

				for i := range threshold {
					if err := exec(ctx, tt.slow(hang)); !errors.Is(err, ErrCircuitBreakerTimeout) {
						t.Errorf("probe %d returned %v, want %v", i+1, err, ErrCircuitBreakerTimeout)
					}
				}

				checkState(t, cb, StateOpen)

				if err := exec(ctx, succeed); !errors.Is(err, ErrCircuitBreakerOpen) {
					t.Errorf("request after the failed probes returned %v, want %v", err, ErrCircuitBreakerOpen)
				}

				// The next half-open round starts from a clean slate.
				time.Sleep(testOpenTimeout)

				for i := range threshold {
					if err := exec(ctx, succeed); err != nil {
						t.Errorf("recovery probe %d returned %v", i+1, err)
					}
				}

				checkState(t, cb, StateClosed)
			})
		})
	}
}

// Regression: the outcome of every request that completed while the breaker was
// half-open was counted as a probe result, including requests that had been
// admitted long before, in CLOSED. One slow straggler could close or reopen the
// breaker on evidence from before the outage.
//
// A request admitted while the breaker was closed can finish after the breaker
// has opened and moved on to half-open. Its outcome says nothing about the
// current probes and must not be counted as one of them.
func TestRegressionStaleRequestCountedAsProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := newTestBreaker(t,
			options.WithThresholdRequestInHalfOpen(2),
			options.WithTargetRateInHalfOpen(1.0),
		)
		ctx := context.Background()

		releaseSlow := make(chan struct{})
		slowDone := make(chan error, 1)

		go func() {
			slowDone <- cb.Execute(ctx, func(context.Context) error {
				<-releaseSlow

				return errBoom
			})
		}()

		synctest.Wait() // the slow request was admitted in CLOSED and is parked

		tripOpen(t, cb)
		time.Sleep(testOpenTimeout)

		if err := cb.Execute(ctx, succeed); err != nil {
			t.Errorf("first probe returned %v", err)
		}
		checkState(t, cb, StateHalfOpen)

		close(releaseSlow)

		if err := <-slowDone; !errors.Is(err, errBoom) {
			t.Errorf("slow request returned %v, want %v", err, errBoom)
		}

		checkState(t, cb, StateHalfOpen)

		if n := cb.GetDetailedStatistics().HalfOpenFailureCount; n == nil || *n != 0 {
			t.Errorf("HalfOpenFailureCount = %v after a stale failure, want 0", deref(n))
		}

		if err := cb.Execute(ctx, succeed); err != nil {
			t.Errorf("second probe returned %v", err)
		}

		// Two probes, two successes: the stale failure did not spoil the round.
		checkState(t, cb, StateClosed)
	})
}

// Half-open decides once thresholdRequestInHalfOpen probes have completed, not
// earlier, however badly the first probes go.
func TestHalfOpenDecidesOnlyAfterThresholdProbes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const threshold = 5

		cb := newTestBreaker(t,
			options.WithThresholdRequestInHalfOpen(threshold),
			options.WithTargetRateInHalfOpen(0.6),
		)
		ctx := context.Background()

		tripOpen(t, cb)
		time.Sleep(testOpenTimeout)

		for i := range threshold - 1 {
			if err := cb.Execute(ctx, fail); !errors.Is(err, errBoom) {
				t.Errorf("probe %d returned %v, want %v", i+1, err, errBoom)
			}

			checkState(t, cb, StateHalfOpen)
		}

		if err := cb.Execute(ctx, fail); !errors.Is(err, errBoom) {
			t.Errorf("last probe returned %v, want %v", err, errBoom)
		}

		checkState(t, cb, StateOpen)
	})
}

// executeFunc is the shape shared by Execute and ExecuteWithTimeout.
type executeFunc func(context.Context, func(context.Context) error) error

// executeVariants lets a test run against both ways of executing a request.
var executeVariants = []struct {
	name string
	pick func(*CircuitBreaker) executeFunc
}{
	{"Execute", func(cb *CircuitBreaker) executeFunc { return cb.Execute }},
	{"ExecuteWithTimeout", func(cb *CircuitBreaker) executeFunc { return cb.ExecuteWithTimeout }},
}

// completionLog collects the outcomes reported to a request complete listener.
type completionLog struct {
	mu  sync.Mutex
	got []bool
}

func (l *completionLog) record(success bool, _ time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.got = append(l.got, success)
}

func (l *completionLog) snapshot() []bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.got)
}

// catchPanic runs f and returns the value it panicked with. It reports a test
// error if f returns normally.
func catchPanic(t *testing.T, f func()) (value any) {
	t.Helper()

	defer func() {
		value = recover()
		if value == nil {
			t.Error("no panic reached the caller")
		}
	}()

	f()

	return nil
}

// panicValue is what the panicking functions below panic with. Comparing the
// recovered value against the same pointer proves the panic was passed on as
// is, neither replaced nor wrapped.
type panicValue struct{ msg string }

// Regression: a request whose fn panicked left no trace. Nothing was recorded
// and the request complete listener never fired, so a dependency that failed by
// panicking could not open the breaker. With ExecuteWithTimeout the panic
// happened in fn's own goroutine and took the program down before the caller
// could see it.
//
// A request whose fn panics is a failed request: it shows up in the statistics
// and in the request complete listener, and the caller still gets the panic.
func TestRegressionPanicCountsAsFailure(t *testing.T) {
	for _, variant := range executeVariants {
		t.Run(variant.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var completions completionLog

				// The threshold keeps the breaker closed, whatever the failure rate.
				cb := newTestBreaker(t,
					options.WithMinimumRequestThreshold(1_000_000),
					options.WithRequestCompleteListener(completions.record),
				)
				exec := variant.pick(cb)
				ctx := context.Background()
				want := &panicValue{"boom"}

				got := catchPanic(t, func() {
					_ = exec(ctx, func(context.Context) error { panic(want) })
				})
				if got != any(want) {
					t.Errorf("caller recovered %#v, want the original panic value %#v", got, want)
				}

				total, successes, failures, _ := cb.GetStatistics()
				if total != 1 || successes != 0 || failures != 1 {
					t.Errorf("statistics = total %d, success %d, failure %d; want 1, 0, 1", total, successes, failures)
				}

				checkState(t, cb, StateClosed)

				synctest.Wait() // the listener has run

				if got, want := completions.snapshot(), []bool{false}; !slices.Equal(got, want) {
					t.Errorf("request complete listener saw %v, want %v", got, want)
				}

				// The breaker keeps working after the panic.
				if err := exec(ctx, succeed); err != nil {
					t.Errorf("request after the panic returned %v", err)
				}
			})
		})
	}
}

// Regression: a probe that panicked was admitted but never completed, so its
// slot was lost and the half-open round could not reach a decision.
//
// In half-open a panicking probe is a failed probe.
func TestRegressionPanicCountsAsFailedProbe(t *testing.T) {
	for _, variant := range executeVariants {
		t.Run(variant.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var completions completionLog

				cb := newTestBreaker(t,
					options.WithThresholdRequestInHalfOpen(2),
					options.WithTargetRateInHalfOpen(1.0),
				)
				exec := variant.pick(cb)
				ctx := context.Background()
				want := &panicValue{"boom"}

				tripOpen(t, cb)
				time.Sleep(testOpenTimeout)

				cb.SetRequestCompleteListener(completions.record)

				got := catchPanic(t, func() {
					_ = exec(ctx, func(context.Context) error { panic(want) })
				})
				if got != any(want) {
					t.Errorf("caller recovered %#v, want the original panic value %#v", got, want)
				}

				// One of the two probes has completed, as a failure.
				checkState(t, cb, StateHalfOpen)

				stats := cb.GetDetailedStatistics()
				if n := stats.HalfOpenFailureCount; n == nil || *n != 1 {
					t.Errorf("HalfOpenFailureCount = %v, want 1", deref(n))
				}
				if n := stats.HalfOpenRequestCount; n == nil || *n != 1 {
					t.Errorf("HalfOpenRequestCount = %v, want 1", deref(n))
				}

				synctest.Wait() // the listener has run

				if got, want := completions.snapshot(), []bool{false}; !slices.Equal(got, want) {
					t.Errorf("request complete listener saw %v, want %v", got, want)
				}

				if err := exec(ctx, succeed); err != nil {
					t.Errorf("second probe returned %v", err)
				}

				// One failure and one success miss the 100% target: reopen.
				checkState(t, cb, StateOpen)
			})
		})
	}
}

// Regression: panics were invisible to the failure rate, so the breaker stayed
// closed in front of a dependency that panicked on every call.
//
// Panics feed the failure rate like any other failure and can open the breaker.
func TestRegressionPanicsCanOpenTheBreaker(t *testing.T) {
	for _, variant := range executeVariants {
		t.Run(variant.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const minimumRequests = 3

				cb := newTestBreaker(t, options.WithMinimumRequestThreshold(minimumRequests))
				exec := variant.pick(cb)
				ctx := context.Background()
				want := &panicValue{"boom"}

				for i := range minimumRequests {
					checkState(t, cb, StateClosed)

					got := catchPanic(t, func() {
						_ = exec(ctx, func(context.Context) error { panic(want) })
					})
					if got != any(want) {
						t.Errorf("panic %d: caller recovered %#v, want the original panic value %#v", i+1, got, want)
					}
				}

				// The third failed request reaches the minimum and trips the breaker.
				checkState(t, cb, StateOpen)

				called := false

				err := exec(ctx, func(context.Context) error {
					called = true

					return nil
				})
				if !errors.Is(err, ErrCircuitBreakerOpen) {
					t.Errorf("request on the opened breaker returned %v, want %v", err, ErrCircuitBreakerOpen)
				}
				if called {
					t.Error("fn was called although the breaker is open")
				}
			})
		})
	}
}

// Regression: a panic in the error predicate skipped the bookkeeping in the
// same way as a panic in fn.
//
// A panic in the error predicate is treated like a panic in fn.
func TestRegressionPanicInErrorPredicateCountsAsFailure(t *testing.T) {
	for _, variant := range executeVariants {
		t.Run(variant.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var completions completionLog

				want := &panicValue{"predicate"}

				cb := newTestBreaker(t,
					options.WithThresholdRequestInHalfOpen(1),
					options.WithRequestCompleteListener(completions.record),
				)
				exec := variant.pick(cb)
				ctx := context.Background()

				tripOpen(t, cb)
				time.Sleep(testOpenTimeout)

				cb.SetCustomErrorPredicate(func(error) bool { panic(want) })

				got := catchPanic(t, func() { _ = exec(ctx, fail) })
				if got != any(want) {
					t.Errorf("caller recovered %#v, want the original panic value %#v", got, want)
				}

				// The only probe of the round failed, so the breaker reopens
				// instead of waiting for a result that never comes.
				checkState(t, cb, StateOpen)

				synctest.Wait() // listeners have run

				// tripOpen's failure, then the probe.
				if got, want := completions.snapshot(), []bool{false, false}; !slices.Equal(got, want) {
					t.Errorf("request complete listener saw %v, want %v", got, want)
				}
			})
		})
	}
}

// Regression: GetDetailedStatistics used to return pointers to the breaker's
// own half-open counters. The caller read them after the lock was released, a
// data race, and the "statistics" it held kept changing with every request.
//
// GetDetailedStatistics returns a snapshot; later requests must not change it.
func TestRegressionDetailedStatisticsLeakedPointers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := newTestBreaker(t,
			options.WithThresholdRequestInHalfOpen(3),
			options.WithTargetRateInHalfOpen(1.0),
		)
		ctx := context.Background()

		tripOpen(t, cb)
		time.Sleep(testOpenTimeout)

		if err := cb.Execute(ctx, succeed); err != nil {
			t.Fatalf("first probe returned %v", err)
		}

		stats := cb.GetDetailedStatistics()
		if stats.HalfOpenSuccessCount == nil || stats.HalfOpenRequestCount == nil {
			t.Fatalf("half-open counters missing in state %s", stats.State)
		}

		if err := cb.Execute(ctx, succeed); err != nil {
			t.Fatalf("second probe returned %v", err)
		}

		if got := *stats.HalfOpenSuccessCount; got != 1 {
			t.Errorf("snapshot HalfOpenSuccessCount = %d after a later request, want 1", got)
		}
		if got := *stats.HalfOpenRequestCount; got != 1 {
			t.Errorf("snapshot HalfOpenRequestCount = %d after a later request, want 1", got)
		}
	})
}

// Listeners run outside the breaker's lock, so they may call back into it.
func TestListenersMayCallBackIntoBreaker(t *testing.T) {
	const openTimeout = time.Millisecond

	events := make(chan string, 8)

	var cb *CircuitBreaker
	cb = newTestBreaker(t,
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(1),
		options.WithStateChangeListener(func(from, to State) {
			_ = cb.GetState()
			_ = cb.GetDetailedStatistics()
			_ = cb.String()
			events <- from.String() + "->" + to.String()
		}),
		options.WithRequestCompleteListener(func(bool, time.Duration) {
			_ = cb.GetState()
			_, _, _, _ = cb.GetStatistics()
		}),
	)

	tripOpen(t, cb)
	time.Sleep(2 * openTimeout) // sleeping longer than the open timeout is always enough

	if err := cb.Execute(context.Background(), succeed); err != nil {
		t.Fatalf("probe returned %v", err)
	}

	// Listeners are delivered asynchronously and in no particular order.
	want := []string{"CLOSED->OPEN", "HALF_OPEN->CLOSED", "OPEN->HALF_OPEN"}
	got := make([]string, 0, len(want))

	// The deadline only bounds a failure (a listener stuck on the breaker's lock).
	deadline := time.After(10 * time.Second)

	for range want {
		select {
		case e := <-events:
			got = append(got, e)
		case <-deadline:
			t.Fatalf("listeners delivered %v, want %v", got, want)
		}
	}

	slices.Sort(got)

	if !slices.Equal(got, want) {
		t.Errorf("transitions = %v, want %v", got, want)
	}
}

// Calling NewCircuitBreaker again with a registered key returns the existing
// breaker untouched: the options of the second call are not applied.
func TestNewCircuitBreakerExistingKeyIgnoresOptions(t *testing.T) {
	key := t.Name()
	t.Cleanup(func() { RemoveCircuitBreaker(key) })

	first := NewCircuitBreaker(key,
		options.WithOpenTimeout(7*time.Second),
		options.WithMinimumRequestThreshold(1),
	)

	second := NewCircuitBreaker(key,
		options.WithOpenTimeout(99*time.Second),
		options.WithMinimumRequestThreshold(1_000),
	)

	if second != first {
		t.Fatalf("second call returned a different breaker (%p), want the registered one (%p)", second, first)
	}

	stats := second.GetDetailedStatistics()
	if stats.OpenTimeoutSeconds != 7 {
		t.Errorf("OpenTimeoutSeconds = %d, want 7 from the first call", stats.OpenTimeoutSeconds)
	}
	if stats.MinimumRequestThreshold != 1 {
		t.Errorf("MinimumRequestThreshold = %d, want 1 from the first call", stats.MinimumRequestThreshold)
	}

	// With the first call's threshold of 1 a single failure opens the breaker;
	// with the second call's threshold of 1000 it would stay closed.
	tripOpen(t, second)

	if cb, ok := GetCircuitBreaker(key); !ok || cb != first {
		t.Errorf("registry holds %p, want %p", cb, first)
	}
}

func deref(n *int64) any {
	if n == nil {
		return "<nil>"
	}

	return *n
}
