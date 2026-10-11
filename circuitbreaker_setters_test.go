package circuitbreaker

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anilonayy/circuitbreaker/options"
)

// The tests in this file pin down that the setters reject the same values the
// options reject, so a breaker cannot be configured into a state it never
// leaves.

// settings is a snapshot of every value a setter can change.
type settings struct {
	timeWindow                 time.Duration
	minimumRequestThreshold    int64
	failureRateThreshold       float64
	openTimeout                time.Duration
	requestTimeout             time.Duration
	thresholdRequestInHalfOpen int64
	targetRateInHalfOpen       float64
}

func snapshotSettings(cb *CircuitBreaker) settings {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	return settings{
		timeWindow:                 cb.timeWindow,
		minimumRequestThreshold:    cb.minimumRequestThreshold,
		failureRateThreshold:       cb.failureRateThreshold,
		openTimeout:                cb.openTimeout,
		requestTimeout:             cb.requestTimeout,
		thresholdRequestInHalfOpen: cb.thresholdRequestInHalfOpen,
		targetRateInHalfOpen:       cb.targetRateInHalfOpen,
	}
}

// Regression: the setters used to store whatever they were given, while the
// options skipped out-of-range values. An invalid value now leaves the setting
// as it was.
func TestRegressionSettersIgnoreInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		set  func(cb *CircuitBreaker)
	}{
		{"SetTimeWindow zero", func(cb *CircuitBreaker) { cb.SetTimeWindow(0) }},
		{"SetTimeWindow negative", func(cb *CircuitBreaker) { cb.SetTimeWindow(-time.Second) }},
		{"SetMinimumRequestThreshold zero", func(cb *CircuitBreaker) { cb.SetMinimumRequestThreshold(0) }},
		{"SetMinimumRequestThreshold negative", func(cb *CircuitBreaker) { cb.SetMinimumRequestThreshold(-1) }},
		{"SetFailureRateThreshold negative", func(cb *CircuitBreaker) { cb.SetFailureRateThreshold(-0.1) }},
		{"SetFailureRateThreshold above one", func(cb *CircuitBreaker) { cb.SetFailureRateThreshold(1.1) }},
		{"SetFailureRateThreshold NaN", func(cb *CircuitBreaker) { cb.SetFailureRateThreshold(math.NaN()) }},
		{"SetOpenTimeout zero", func(cb *CircuitBreaker) { cb.SetOpenTimeout(0) }},
		{"SetOpenTimeout negative", func(cb *CircuitBreaker) { cb.SetOpenTimeout(-time.Second) }},
		{"SetRequestTimeout zero", func(cb *CircuitBreaker) { cb.SetRequestTimeout(0) }},
		{"SetRequestTimeout negative", func(cb *CircuitBreaker) { cb.SetRequestTimeout(-time.Second) }},
		{"SetThresholdRequestInHalfOpen zero", func(cb *CircuitBreaker) { cb.SetThresholdRequestInHalfOpen(0) }},
		{"SetThresholdRequestInHalfOpen negative", func(cb *CircuitBreaker) { cb.SetThresholdRequestInHalfOpen(-1) }},
		{"SetTargetRateInHalfOpen negative", func(cb *CircuitBreaker) { cb.SetTargetRateInHalfOpen(-0.1) }},
		{"SetTargetRateInHalfOpen above one", func(cb *CircuitBreaker) { cb.SetTargetRateInHalfOpen(1.1) }},
		{"SetTargetRateInHalfOpen NaN", func(cb *CircuitBreaker) { cb.SetTargetRateInHalfOpen(math.NaN()) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newTestBreaker(t)
			before := snapshotSettings(cb)

			tt.set(cb)

			if after := snapshotSettings(cb); after != before {
				t.Fatalf("settings changed: before %+v, after %+v", before, after)
			}
		})
	}
}

// The valid ends of each range are still accepted.
func TestSettersAcceptValidValues(t *testing.T) {
	cb := newTestBreaker(t)

	cb.SetTimeWindow(2 * time.Minute)
	cb.SetMinimumRequestThreshold(1)
	cb.SetFailureRateThreshold(0)
	cb.SetOpenTimeout(time.Millisecond)
	cb.SetRequestTimeout(time.Millisecond)
	cb.SetThresholdRequestInHalfOpen(1)
	cb.SetTargetRateInHalfOpen(1)

	want := settings{
		timeWindow:                 2 * time.Minute,
		minimumRequestThreshold:    1,
		failureRateThreshold:       0,
		openTimeout:                time.Millisecond,
		requestTimeout:             time.Millisecond,
		thresholdRequestInHalfOpen: 1,
		targetRateInHalfOpen:       1,
	}
	if got := snapshotSettings(cb); got != want {
		t.Fatalf("settings: got %+v, want %+v", got, want)
	}
}

// Regression: SetThresholdRequestInHalfOpen(0) left a half-open breaker with no
// probe slots, so it rejected every request and never left the state.
func TestRegressionZeroProbeThresholdDoesNotLockHalfOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const openTimeout = 10 * time.Second

		cb := newTestBreaker(t,
			options.WithMinimumRequestThreshold(2),
			options.WithOpenTimeout(openTimeout),
			options.WithThresholdRequestInHalfOpen(2),
			options.WithTargetRateInHalfOpen(0.5),
		)
		ctx := context.Background()

		for range 2 {
			if err := cb.Execute(ctx, fail); !errors.Is(err, errBoom) {
				t.Fatalf("Execute: got %v, want errBoom", err)
			}
		}

		if got := cb.GetState(); got != StateOpen {
			t.Fatalf("state after failures: got %v, want %v", got, StateOpen)
		}

		cb.SetThresholdRequestInHalfOpen(0)
		time.Sleep(openTimeout)

		for range 2 {
			if err := cb.Execute(ctx, func(context.Context) error { return nil }); err != nil {
				t.Fatalf("probe: got %v, want nil", err)
			}
		}

		if got := cb.GetState(); got != StateClosed {
			t.Fatalf("state after successful probes: got %v, want %v", got, StateClosed)
		}
	})
}

// Regression: SetCustomErrorPredicate(nil) made the next failing request panic
// with a nil function call. A nil predicate now leaves the current one in place.
func TestRegressionNilErrorPredicateIsIgnored(t *testing.T) {
	cb := newTestBreaker(t)

	cb.SetCustomErrorPredicate(nil)

	if err := cb.Execute(context.Background(), fail); !errors.Is(err, errBoom) {
		t.Fatalf("Execute: got %v, want errBoom", err)
	}

	if _, _, failures, _ := cb.GetStatistics(); failures != 1 {
		t.Fatalf("failure count: got %d, want 1", failures)
	}
}

// Regression: SetStateChangeListener used to drop a value of the wrong type
// without a word. A mistyped listener now panics, and a defined type with the
// right signature is accepted.
func TestRegressionStateChangeListenerType(t *testing.T) {
	type namedListener func(from, to State)

	t.Run("a defined func type is accepted", func(t *testing.T) {
		cb := newTestBreaker(t)
		called := make(chan struct{}, 1)

		cb.SetStateChangeListener(namedListener(func(_, _ State) { called <- struct{}{} }))

		_ = cb.Execute(context.Background(), fail)

		select {
		case <-called:
		case <-time.After(10 * time.Second):
			t.Fatal("the listener was not called")
		}
	})

	t.Run("nil keeps the earlier listener", func(t *testing.T) {
		cb := newTestBreaker(t)
		called := make(chan struct{}, 1)

		cb.SetStateChangeListener(func(_, _ State) { called <- struct{}{} })
		cb.SetStateChangeListener(nil)

		_ = cb.Execute(context.Background(), fail)

		select {
		case <-called:
		case <-time.After(10 * time.Second):
			t.Fatal("the listener was not called")
		}
	})

	wrong := map[string]any{
		"string parameters": func(from, to string) {},
		"one parameter":     func(to State) {},
		"returns a value":   func(from, to State) error { return nil },
		"not a func":        42,
	}

	for name, listener := range wrong {
		t.Run(name+" panics", func(t *testing.T) {
			cb := newTestBreaker(t)

			defer func() {
				if recover() == nil {
					t.Fatal("SetStateChangeListener did not panic")
				}

				// The panic must not leave the breaker locked.
				if got := cb.GetState(); got != StateClosed {
					t.Fatalf("state: got %v, want %v", got, StateClosed)
				}
			}()

			cb.SetStateChangeListener(listener)
		})
	}
}
