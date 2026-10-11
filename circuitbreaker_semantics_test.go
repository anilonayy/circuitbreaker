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

// A caller that cancels its context is treated differently by the two execute
// methods. Execute waits for fn and passes fn's error on, so the caller sees
// context.Canceled and the error predicate decides whether it is a failure.
// ExecuteWithTimeout stops waiting as soon as the context is done, for whatever
// reason, and reports that as ErrCircuitBreakerTimeout and as a failure.
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
			if !errors.Is(err, ErrCircuitBreakerTimeout) {
				t.Errorf("ExecuteWithTimeout returned %v, want %v", err, ErrCircuitBreakerTimeout)
			}

			if got := time.Since(start); got != time.Second {
				t.Errorf("ExecuteWithTimeout returned after %v, want 1s", got)
			}

			if total, _, failures, _ := cb.GetStatistics(); total != 1 || failures != 1 {
				t.Errorf("statistics = total %d, failures %d; want 1, 1", total, failures)
			}
		})
	})
}
