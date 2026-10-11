package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anilonayy/circuitbreaker/options"
)

// The tests in this file pin down how the breaker reads time: what leaves the
// sliding window, and which context errors it reports as a timeout.

// Outcomes older than the time window no longer count, neither in the
// statistics nor towards opening the breaker.
func TestOutcomesLeaveTheTimeWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const window = 10 * time.Second

		cb := newTestBreaker(t,
			options.WithTimeWindow(window),
			options.WithMinimumRequestThreshold(4),
		)
		ctx := context.Background()

		for range 3 {
			if err := cb.Execute(ctx, fail); !errors.Is(err, errBoom) {
				t.Fatalf("Execute returned %v, want %v", err, errBoom)
			}
		}

		if total, _, failures, _ := cb.GetStatistics(); total != 3 || failures != 3 {
			t.Fatalf("statistics = total %d, failures %d; want 3, 3", total, failures)
		}

		// Still inside the window: the three failures are counted.
		time.Sleep(window / 2)

		if total, _, _, _ := cb.GetStatistics(); total != 3 {
			t.Errorf("total = %d halfway through the window, want 3", total)
		}

		// Past the window they are gone, even before a new request cleans up.
		time.Sleep(window)

		if total, _, _, rate := cb.GetStatistics(); total != 0 || rate != 0 {
			t.Errorf("statistics = total %d, failure rate %v after the window, want 0, 0", total, rate)
		}

		// A fourth failure would have opened the breaker had the first three
		// still counted. Recording it also drops their buckets for good.
		if err := cb.Execute(ctx, fail); !errors.Is(err, errBoom) {
			t.Fatalf("Execute returned %v, want %v", err, errBoom)
		}

		checkState(t, cb, StateClosed)

		if total, _, failures, _ := cb.GetStatistics(); total != 1 || failures != 1 {
			t.Errorf("statistics = total %d, failures %d after the window, want 1, 1", total, failures)
		}
	})
}

// The breaker recognizes a timeout by the error, not by its own timer: any
// error that wraps context.DeadlineExceeded is reported as
// ErrCircuitBreakerTimeout and counted as a failure, including one from a
// deadline fn set up itself and one from a deadline on the caller's context.
// The custom error predicate is not asked.
func TestDeadlineErrorsAreReportedAsTimeout(t *testing.T) {
	errInner := fmt.Errorf("query: %w", context.DeadlineExceeded)

	for _, variant := range executeVariants {
		t.Run(variant.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				predicateCalls := 0

				cb := newTestBreaker(t,
					options.WithMinimumRequestThreshold(1_000_000),
					options.WithCustomErrorPredicate(func(error) bool {
						predicateCalls++

						return false
					}),
				)
				exec := variant.pick(cb)

				// fn fails at once with its own deadline error; the breaker's
				// request timeout of an hour is nowhere near.
				err := exec(context.Background(), func(context.Context) error { return errInner })
				if !errors.Is(err, ErrCircuitBreakerTimeout) {
					t.Errorf("own deadline: returned %v, want %v", err, ErrCircuitBreakerTimeout)
				}

				// The caller's context expires long before the request timeout.
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				err = exec(ctx, func(ctx context.Context) error {
					<-ctx.Done()

					return ctx.Err()
				})
				if !errors.Is(err, ErrCircuitBreakerTimeout) {
					t.Errorf("caller deadline: returned %v, want %v", err, ErrCircuitBreakerTimeout)
				}

				if total, _, failures, _ := cb.GetStatistics(); total != 2 || failures != 2 {
					t.Errorf("statistics = total %d, failures %d; want 2, 2", total, failures)
				}

				if predicateCalls != 0 {
					t.Errorf("error predicate was asked %d times about a timeout, want 0", predicateCalls)
				}
			})
		})
	}
}

// A caller that cancels its context gets context.Canceled from both execute
// methods, and the error predicate decides whether it is a failure. Execute
// waits for fn and passes fn's error on; ExecuteWithTimeout stops waiting as
// soon as the context is done and reports the cancellation itself.
//
// Regression: ExecuteWithTimeout used to report a cancellation as
// ErrCircuitBreakerTimeout and always count it as a failure.
func TestCallerCancellation(t *testing.T) {
	cancelAfter := func(d time.Duration) context.Context {
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(d)
			cancel()
		}()

		return ctx
	}

	t.Run("Execute", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cb := newTestBreaker(t, options.WithMinimumRequestThreshold(1_000_000))

			err := cb.Execute(cancelAfter(time.Second), func(ctx context.Context) error {
				<-ctx.Done()

				return ctx.Err()
			})
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Execute returned %v, want %v", err, context.Canceled)
			}

			// The default predicate counts every error, this one included.
			if total, _, failures, _ := cb.GetStatistics(); total != 1 || failures != 1 {
				t.Errorf("statistics = total %d, failures %d; want 1, 1", total, failures)
			}
		})
	})

	t.Run("ExecuteWithTimeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cb := newTestBreaker(t, options.WithMinimumRequestThreshold(1_000_000))

			hang := make(chan struct{})
			defer close(hang)

			start := time.Now()

			err := cb.ExecuteWithTimeout(cancelAfter(time.Second), func(context.Context) error {
				<-hang

				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Errorf("ExecuteWithTimeout returned %v, want %v", err, context.Canceled)
			}

			if errors.Is(err, ErrCircuitBreakerTimeout) {
				t.Errorf("ExecuteWithTimeout reported a cancellation as a timeout: %v", err)
			}

			if got := time.Since(start); got != time.Second {
				t.Errorf("ExecuteWithTimeout returned after %v, want 1s", got)
			}

			// The default predicate counts every error, this one included.
			if total, _, failures, _ := cb.GetStatistics(); total != 1 || failures != 1 {
				t.Errorf("statistics = total %d, failures %d; want 1, 1", total, failures)
			}
		})
	})

	t.Run("ExecuteWithTimeout asks the error predicate", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cb := newTestBreaker(t,
				options.WithMinimumRequestThreshold(1_000_000),
				options.WithCustomErrorPredicate(func(err error) bool {
					return !errors.Is(err, context.Canceled)
				}),
			)

			hang := make(chan struct{})
			defer close(hang)

			err := cb.ExecuteWithTimeout(cancelAfter(time.Second), func(context.Context) error {
				<-hang

				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Errorf("ExecuteWithTimeout returned %v, want %v", err, context.Canceled)
			}

			if total, _, failures, _ := cb.GetStatistics(); total != 1 || failures != 0 {
				t.Errorf("statistics = total %d, failures %d; want 1, 0", total, failures)
			}
		})
	})
}

// Regression: the probes of a half-open round stayed in the sliding window
// after the breaker closed. With a failure rate threshold below the share of
// failed probes a round may contain, the first request after closing, even a
// successful one, opened the breaker again. Closing now starts a fresh window.
func TestRegressionProbeResultsDoNotReopenAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const openTimeout = 10 * time.Second

		cb := newTestBreaker(t,
			options.WithMinimumRequestThreshold(2),
			options.WithFailureRateThreshold(0.3),
			options.WithOpenTimeout(openTimeout),
			options.WithThresholdRequestInHalfOpen(5),
			options.WithTargetRateInHalfOpen(0.6),
		)
		ctx := context.Background()
		succeed := func(context.Context) error { return nil }

		for range 2 {
			_ = cb.Execute(ctx, fail)
		}

		if got := cb.GetState(); got != StateOpen {
			t.Fatalf("state after failures: got %v, want %v", got, StateOpen)
		}

		time.Sleep(openTimeout)

		// Three of five probes succeed: exactly the target rate, so it closes.
		for _, probe := range []func(context.Context) error{succeed, fail, succeed, fail, succeed} {
			_ = cb.Execute(ctx, probe)
		}

		if got := cb.GetState(); got != StateClosed {
			t.Fatalf("state after probes: got %v, want %v", got, StateClosed)
		}

		if total, _, failures, _ := cb.GetStatistics(); total != 0 || failures != 0 {
			t.Errorf("statistics after closing = total %d, failures %d; want 0, 0", total, failures)
		}

		if err := cb.Execute(ctx, succeed); err != nil {
			t.Fatalf("first request after closing: got %v, want nil", err)
		}

		if got := cb.GetState(); got != StateClosed {
			t.Fatalf("state after a successful request: got %v, want %v", got, StateClosed)
		}
	})
}
