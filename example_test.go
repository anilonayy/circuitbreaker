package circuitbreaker_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	circuitbreaker "github.com/anilonayy/circuitbreaker"
	"github.com/anilonayy/circuitbreaker/options"
)

// The examples remove their breaker from the registry when they are done. A
// program normally keeps its breakers for as long as it runs; the examples do
// it so that they can be run again in the same process.

var errUnavailable = errors.New("inventory: connection refused")

// A breaker wraps a call. As long as the call succeeds it stays closed and
// passes the result through.
func Example() {
	cb := circuitbreaker.NewCircuitBreaker("inventory")
	defer circuitbreaker.RemoveCircuitBreaker("inventory")

	err := cb.Execute(context.Background(), func(ctx context.Context) error {
		// Call the dependency here and pass ctx on: it carries the breaker's
		// request timeout.
		return nil
	})

	fmt.Println("error:", err)
	fmt.Println("state:", cb.GetState())
	// Output:
	// error: <nil>
	// state: CLOSED
}

// Once the failure rate in the time window reaches the threshold, the breaker
// opens and rejects requests without calling the function.
func ExampleCircuitBreaker_Execute() {
	cb := circuitbreaker.NewCircuitBreaker("inventory-execute",
		options.WithMinimumRequestThreshold(4), // judge after 4 requests
		options.WithFailureRateThreshold(0.5),  // open at 50% failures
	)
	defer circuitbreaker.RemoveCircuitBreaker("inventory-execute")

	ctx := context.Background()
	failing := func(context.Context) error { return errUnavailable }

	for i := 1; i <= 4; i++ {
		err := cb.Execute(ctx, failing)
		fmt.Printf("request %d: %v (%s)\n", i, err, cb.GetState())
	}

	called := false
	err := cb.Execute(ctx, func(context.Context) error {
		called = true

		return nil
	})

	fmt.Printf("request 5: %v (function called: %t)\n", err, called)
	// Output:
	// request 1: inventory: connection refused (CLOSED)
	// request 2: inventory: connection refused (CLOSED)
	// request 3: inventory: connection refused (CLOSED)
	// request 4: inventory: connection refused (OPEN)
	// request 5: circuit breaker is open (function called: false)
}

// ExecuteWithTimeout returns when the request timeout expires even if the
// function does not watch its context.
func ExampleCircuitBreaker_ExecuteWithTimeout() {
	cb := circuitbreaker.NewCircuitBreaker("inventory-timeout",
		options.WithRequestTimeout(20*time.Millisecond),
	)
	defer circuitbreaker.RemoveCircuitBreaker("inventory-timeout")

	done := make(chan struct{})
	defer close(done)

	err := cb.ExecuteWithTimeout(context.Background(), func(context.Context) error {
		<-done // a call that hangs and ignores its context

		return nil
	})

	fmt.Println(err)
	fmt.Println(errors.Is(err, circuitbreaker.ErrCircuitBreakerTimeout))

	_, _, failures, _ := cb.GetStatistics()
	fmt.Println("failures:", failures)
	// Output:
	// circuit breaker request timeout
	// true
	// failures: 1
}

// Breakers are shared by key. A second NewCircuitBreaker call with the same key
// returns the first breaker and does not apply its options.
func ExampleNewCircuitBreaker() {
	first := circuitbreaker.NewCircuitBreaker("search", options.WithOpenTimeout(10*time.Second))
	defer circuitbreaker.RemoveCircuitBreaker("search")

	second := circuitbreaker.NewCircuitBreaker("search", options.WithOpenTimeout(99*time.Second))

	fmt.Println("same breaker:", first == second)
	fmt.Println("open timeout:", second.GetDetailedStatistics().OpenTimeoutSeconds)

	// To reconfigure a registered breaker, use its setters.
	second.SetOpenTimeout(99 * time.Second)
	fmt.Println("open timeout:", first.GetDetailedStatistics().OpenTimeoutSeconds)
	// Output:
	// same breaker: true
	// open timeout: 10
	// open timeout: 99
}

// Start from a preset and append overrides; later options win.
func Example_options() {
	opts := append(options.WithHTTPDefaults(),
		options.WithFailureRateThreshold(0.3),
		options.WithRequestTimeout(2*time.Second),
		options.WithThresholdRequestInHalfOpen(3),
	)

	cb := circuitbreaker.NewCircuitBreaker("catalog", opts...)
	defer circuitbreaker.RemoveCircuitBreaker("catalog")

	cfg := cb.GetDetailedStatistics()
	fmt.Println("time window (s):", cfg.TimeWindowSeconds)
	fmt.Println("minimum requests:", cfg.MinimumRequestThreshold)
	fmt.Println("failure rate threshold:", cfg.FailureRateThreshold)
	fmt.Println("open timeout (s):", cfg.OpenTimeoutSeconds)
	fmt.Println("request timeout (s):", cfg.RequestTimeoutSeconds)
	fmt.Println("half-open probes:", cfg.ThresholdRequestInHalfOpen)
	fmt.Println("half-open target rate:", cfg.TargetRateInHalfOpen)
	// Output:
	// time window (s): 60
	// minimum requests: 10
	// failure rate threshold: 0.3
	// open timeout (s): 30
	// request timeout (s): 2
	// half-open probes: 3
	// half-open target rate: 0.6
}

// The builder is an alternative way to put options together. It takes
// durations in seconds.
func ExampleConfigBuilder() {
	opts := options.NewConfigBuilder().
		TimeWindow(120).
		MinimumRequestThreshold(20).
		FailureRateThreshold(0.4).
		OpenTimeout(45).
		Build()

	cb := circuitbreaker.NewCircuitBreaker("reports", opts...)
	defer circuitbreaker.RemoveCircuitBreaker("reports")

	cfg := cb.GetDetailedStatistics()
	fmt.Println(cfg.TimeWindowSeconds, cfg.MinimumRequestThreshold, cfg.FailureRateThreshold, cfg.OpenTimeoutSeconds)
	// Output:
	// 120 20 0.4 45
}

var errNotFound = errors.New("user not found")

// An error predicate keeps errors that say nothing about the health of the
// dependency out of the failure rate. The caller still gets the error.
func Example_errorPredicate() {
	cb := circuitbreaker.NewCircuitBreaker("users",
		options.WithMinimumRequestThreshold(2),
		options.WithCustomErrorPredicate(func(err error) bool {
			return !errors.Is(err, errNotFound)
		}),
	)
	defer circuitbreaker.RemoveCircuitBreaker("users")

	var err error
	for range 5 {
		err = cb.Execute(context.Background(), func(context.Context) error { return errNotFound })
	}

	total, successes, failures, _ := cb.GetStatistics()

	fmt.Println("error:", err)
	fmt.Printf("total=%d successes=%d failures=%d\n", total, successes, failures)
	fmt.Println("state:", cb.GetState())
	// Output:
	// error: user not found
	// total=5 successes=5 failures=0
	// state: CLOSED
}

// statusFor maps the result of Execute to an HTTP status for the client of a
// service that sits in front of the protected dependency.
func statusFor(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen):
		// Rejected without trying: tell the client to back off.
		return http.StatusServiceUnavailable
	case errors.Is(err, circuitbreaker.ErrCircuitBreakerTimeout):
		return http.StatusGatewayTimeout
	default:
		// The dependency answered with an error of its own.
		return http.StatusBadGateway
	}
}

// The two sentinel errors tell a rejected request from a slow one; everything
// else is the dependency's own error. Compare with errors.Is.
func Example_errorMapping() {
	cb := circuitbreaker.NewCircuitBreaker("pricing",
		options.WithMinimumRequestThreshold(3),
		options.WithRequestTimeout(20*time.Millisecond),
	)
	defer circuitbreaker.RemoveCircuitBreaker("pricing")

	ctx := context.Background()

	ok := func(context.Context) error { return nil }
	broken := func(context.Context) error { return errUnavailable }
	slow := func(ctx context.Context) error {
		<-ctx.Done() // gives up when the request timeout expires

		return ctx.Err()
	}

	// One success, one error, one timeout: two failures out of three requests
	// open the breaker, and the fourth request is rejected.
	for _, call := range []func(context.Context) error{ok, broken, slow, ok} {
		err := cb.Execute(ctx, call)
		fmt.Printf("%d  %v\n", statusFor(err), err)
	}
	// Output:
	// 200  <nil>
	// 502  inventory: connection refused
	// 504  circuit breaker request timeout
	// 503  circuit breaker is open
}

// After the open timeout the breaker lets a fixed number of probes through and
// closes again if enough of them succeed.
func Example_recovery() {
	cb := circuitbreaker.NewCircuitBreaker("shipping",
		options.WithMinimumRequestThreshold(1),
		options.WithOpenTimeout(20*time.Millisecond),
		options.WithThresholdRequestInHalfOpen(2), // two probes per round
		options.WithTargetRateInHalfOpen(1.0),     // both must succeed
	)
	defer circuitbreaker.RemoveCircuitBreaker("shipping")

	ctx := context.Background()
	ok := func(context.Context) error { return nil }

	_ = cb.Execute(ctx, func(context.Context) error { return errUnavailable })
	fmt.Println("after a failure:", cb.GetState())

	time.Sleep(40 * time.Millisecond) // wait out the open timeout

	_ = cb.Execute(ctx, ok)
	fmt.Println("after the first probe:", cb.GetState())

	_ = cb.Execute(ctx, ok)
	fmt.Println("after the second probe:", cb.GetState())
	// Output:
	// after a failure: OPEN
	// after the first probe: HALF_OPEN
	// after the second probe: CLOSED
}

// Listeners are called asynchronously, each call in its own goroutine. This
// example waits for the call before it prints.
func Example_stateChangeListener() {
	changes := make(chan string, 1)

	cb := circuitbreaker.NewCircuitBreaker("billing",
		options.WithMinimumRequestThreshold(1),
		// The listener must have exactly this signature.
		options.WithStateChangeListener(func(from, to circuitbreaker.State) {
			changes <- fmt.Sprintf("%s -> %s", from, to)
		}),
	)
	defer circuitbreaker.RemoveCircuitBreaker("billing")

	_ = cb.Execute(context.Background(), func(context.Context) error { return errUnavailable })

	fmt.Println(<-changes)
	// Output:
	// CLOSED -> OPEN
}

// GetDetailedStatistics returns a snapshot that marshals to JSON, for a status
// endpoint or a log line.
func ExampleCircuitBreaker_GetDetailedStatistics() {
	cb := circuitbreaker.NewCircuitBreaker("ledger")
	defer circuitbreaker.RemoveCircuitBreaker("ledger")

	ctx := context.Background()

	for i := range 4 {
		_ = cb.Execute(ctx, func(context.Context) error {
			if i == 0 {
				return errUnavailable
			}

			return nil
		})
	}

	stats := cb.GetDetailedStatistics()

	fmt.Println(stats.Key, stats.State)
	fmt.Printf("total=%d successes=%d failures=%d rate=%.2f\n",
		stats.TotalRequests, stats.SuccessCount, stats.FailureCount, stats.FailureRate)
	fmt.Println(cb)
	// Output:
	// ledger CLOSED
	// total=4 successes=3 failures=1 rate=0.25
	// CircuitBreaker{key=ledger, state=CLOSED, requests=4, failures=1, failureRate=25.00%}
}
