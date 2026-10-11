// Package circuitbreaker implements a circuit breaker for calls to things that
// can fail or hang: HTTP services, databases, RPC peers.
//
// A breaker wraps each call. While the calls succeed it stays out of the way.
// When too many of them fail it stops making them for a while and fails fast
// instead, then lets a limited number of probe requests through to find out
// whether the dependency has recovered.
//
//	cb := circuitbreaker.NewCircuitBreaker("payments",
//		options.WithFailureRateThreshold(0.5),
//		options.WithOpenTimeout(30*time.Second),
//	)
//
//	err := cb.Execute(ctx, func(ctx context.Context) error {
//		return callPayments(ctx)
//	})
//
// Configuration lives in the options subpackage. A breaker built without
// options uses a 1 minute time window, a minimum of 10 requests, a failure rate
// threshold of 0.5, an open timeout of 30 seconds, a request timeout of 5
// seconds, 10 half-open probes and a half-open target rate of 0.6.
//
// # States
//
// A breaker is in one of three states.
//
// [StateClosed] is the normal state. Every request is admitted. Outcomes are
// counted in one-second buckets over a sliding time window. As soon as the
// window holds at least the minimum number of requests and the share of
// failures among them reaches the failure rate threshold, the breaker opens.
// The check runs when a request completes.
//
// [StateOpen] rejects every request with [ErrCircuitBreakerOpen] without
// calling the wrapped function. It lasts for the open timeout. The breaker has
// no timer of its own: it moves on when the first request arrives after the
// open timeout has passed, so a breaker nobody calls keeps reporting OPEN.
//
// [StateHalfOpen] is the trial. Entering it clears the statistics. The breaker
// admits a fixed number of probe requests (see below) and, once all of them
// have completed, closes if the share of successful probes reaches the target
// rate and opens again otherwise. Closing clears the statistics as well: the
// closed breaker starts with an empty window, so the probes that closed it,
// failed ones included, do not count towards opening it again.
//
// # Half-open probe limit
//
// In half-open at most ThresholdRequestInHalfOpen requests are admitted per
// round. A probe takes its slot when it is admitted, not when it completes, so
// the limit holds for requests in flight: if a hundred callers arrive at the
// moment the open timeout expires, the configured number get through and the
// rest receive [ErrCircuitBreakerOpen]. Callers therefore see that error in
// two states, and it means the same in both: the function was not called.
//
// The round is decided only when every admitted probe has completed. Nothing
// is decided early, however the first probes go.
//
// Only probes are counted. A request that was admitted while the breaker was
// still closed and finishes during a later half-open round is recorded in the
// statistics, but it is not a probe result and cannot close or reopen the
// breaker.
//
// # Timeouts
//
// The function passed to [CircuitBreaker.Execute] receives a context that is
// canceled when the request timeout expires. Execute itself waits until the
// function returns, so the function has to honor that context; one that
// ignores it and returns nil late is a success.
//
// [CircuitBreaker.ExecuteWithTimeout] does not depend on that cooperation. It
// runs the function in a separate goroutine and returns when the context is
// done, leaving the function to finish in the background with its result
// discarded.
//
// A timeout is a failed request. It is counted in the statistics, reported to
// the request complete listener with success=false and, in half-open, counted
// as a failed probe, so a round of probes that all hang ends in OPEN. The
// caller receives [ErrCircuitBreakerTimeout].
//
// The breaker recognizes a timeout by the error, not by its own clock: any
// error that wraps [context.DeadlineExceeded] is turned into
// ErrCircuitBreakerTimeout and counted as a failure without asking the error
// predicate. That includes a deadline the function set up itself and a deadline
// on the caller's context. A caller that cancels its context is not a timeout:
// both methods return [context.Canceled] and let the error predicate judge it.
// Execute gets there by returning the function's error, so it depends on the
// function honoring its context; ExecuteWithTimeout stops waiting as soon as
// the context is canceled and reports the cancellation itself.
//
// # Errors and the error predicate
//
// Any other error the function returns is passed back to the caller unchanged.
// Whether it counts as a failure is up to the error predicate, which by default
// says yes to every non-nil error. Use options.WithCustomErrorPredicate to
// keep errors that say nothing about the health of the dependency, such as a
// "not found", from opening the breaker.
//
// # Panics
//
// If the function or the error predicate panics, the request is recorded as a
// failure exactly like a returned error: in the statistics, as a failed probe
// in half-open, and in the request complete listener. The breaker does not
// recover the panic on the caller's behalf; it reaches the caller with its
// original value. With ExecuteWithTimeout the panic happens in the function's
// goroutine and is raised again in the caller's. If the caller has already
// left because of a timeout, there is nobody to hand it to, and it terminates
// the program like any unrecovered panic in a goroutine.
//
// # Listeners
//
// Two callbacks can be registered: a state change listener, called with the
// old and the new state on every transition, and a request complete listener,
// called with the outcome and duration of every admitted request. Rejected
// requests are not reported.
//
// Each call runs in a goroutine of its own, outside the breaker's lock. A
// listener may therefore call back into the breaker, and a slow listener does
// not hold up requests. The price is that calls are not ordered: two
// transitions in quick succession can reach the listener in either order, and
// a listener may still be running after the request that triggered it has
// returned. A listener that needs the current state should ask for it with
// [CircuitBreaker.GetState] rather than keep track of the transitions it saw.
// A panic in a listener is not recovered and terminates the program.
//
// The state change listener is passed as any, because the options package
// cannot name [State]. It must be a func(from, to State), or a value of a
// defined type with that signature. A nil value is ignored and keeps the
// listener set earlier; a value of any other type panics when it is set, so a
// mistyped listener shows up when the breaker is created, not as a callback
// that never fires.
//
// # The registry: one breaker per key
//
// Breakers are registered by key in a process-wide registry, so that every
// part of a program that names the same dependency shares one breaker.
// [NewCircuitBreaker] with a key that is already registered returns the
// existing breaker and ignores the options of that call, without an error and
// even if they differ. To change a registered breaker, call its Set methods; to
// start over, call [RemoveCircuitBreaker] first. Removing a breaker only takes
// it out of the registry; code that still holds the pointer keeps using it.
//
// Entries are never removed automatically. Keys should name dependencies, not
// requests: a key built from a user ID or a URL with parameters grows the
// registry without bound.
//
// # Concurrency
//
// All methods of a [CircuitBreaker] and all package functions are safe for
// concurrent use. One mutex per breaker guards its state, configuration and
// statistics. Admitting a request and recording its outcome are one critical
// section each, so concurrent callers observe a single consistent state
// machine: one transition per cause and one shared set of probe slots. The
// wrapped function, the error predicate and the listeners run outside the
// lock.
//
// The Set methods may be called while requests are in flight. A request keeps
// the request timeout and the error predicate it was admitted with.
//
// The Set methods store what they are given. The range checks (a positive
// duration, a rate between 0 and 1, a non-nil predicate) are done by the
// functions in the options package, which skip a value that fails them.
package circuitbreaker
