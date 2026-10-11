package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/anilonayy/circuitbreaker/options"
)

// State is the state of a circuit breaker. The package documentation describes
// what each state does and how a breaker moves between them.
type State int

const (
	// StateClosed is the normal operating state: requests pass through and
	// their outcomes are tracked. It is the state a new breaker starts in.
	StateClosed State = iota
	// StateOpen means the circuit is open: every request is rejected with
	// ErrCircuitBreakerOpen until the open timeout has passed.
	StateOpen
	// StateHalfOpen means a limited number of probe requests is let through to
	// find out whether the dependency has recovered; all others are rejected.
	StateHalfOpen
)

// String returns the name of the state: "CLOSED", "OPEN" or "HALF_OPEN", and
// "UNKNOWN" for any other value.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// RequestStats holds the request counters of one second of the sliding window.
type RequestStats struct {
	RequestCount int64 // Total number of requests in that second
	SuccessCount int64 // Number of successful requests in that second
	FailureCount int64 // Number of failed requests in that second
}

// DetailedStatistics describes a circuit breaker at one moment: its state, its
// configuration and the counters of the current time window. It marshals to
// JSON, so it can be served from a status endpoint as it is.
//
// It is a snapshot: the values, including the ones behind pointers, do not
// change after GetDetailedStatistics returns.
//
// The pointer fields depend on the state and are nil (omitted from JSON) in the
// states they do not apply to: OpenedAt, NextHalfOpenAt and
// TimeUntilHalfOpenSeconds are set while the breaker is open, the HalfOpen
// fields and RemainingTestRequests while it is half-open.
//
// The fields ending in Seconds are whole seconds; a shorter duration, such as a
// request timeout of 500ms, is reported as 0.
type DetailedStatistics struct {
	// OpenedAt is when the breaker opened, in RFC 3339 format.
	OpenedAt *string `json:"opened_at,omitempty"`
	// HalfOpenSuccessRate is the share of successful probes among the completed
	// probes of the current round. It is nil until a probe has completed.
	HalfOpenSuccessRate *float64 `json:"half_open_success_rate,omitempty"`
	// HalfOpenFailureCount is the number of completed probes that failed.
	HalfOpenFailureCount *int64 `json:"half_open_failure_count,omitempty"`
	// HalfOpenSuccessCount is the number of completed probes that succeeded.
	HalfOpenSuccessCount *int64 `json:"half_open_success_count,omitempty"`
	// RemainingTestRequests is the number of probe slots still free in the
	// current round.
	RemainingTestRequests *int64 `json:"remaining_test_requests,omitempty"`
	// HalfOpenRequestCount is the number of probes admitted in the current
	// round, in flight or completed.
	HalfOpenRequestCount *int64 `json:"half_open_request_count,omitempty"`
	// TimeUntilHalfOpenSeconds is the time left until the open timeout has
	// passed. It is nil once it has: the breaker then turns half-open with the
	// next request.
	TimeUntilHalfOpenSeconds *int64 `json:"time_until_half_open_seconds,omitempty"`
	// NextHalfOpenAt is OpenedAt plus the open timeout, in RFC 3339 format.
	NextHalfOpenAt *string `json:"next_half_open_at,omitempty"`
	// State is the state's name as returned by State.String.
	State string `json:"state"`
	// Key is the key the breaker is registered under.
	Key string `json:"key"`
	// FailureRate is FailureCount divided by TotalRequests, 0 without requests.
	FailureRate float64 `json:"failure_rate"`
	// ThresholdRequestInHalfOpen is the number of probes per half-open round.
	ThresholdRequestInHalfOpen int64 `json:"threshold_request_in_half_open"`
	// TargetRateInHalfOpen is the share of successful probes needed to close.
	TargetRateInHalfOpen float64 `json:"target_rate_in_half_open"`
	// RequestTimeoutSeconds is the request timeout.
	RequestTimeoutSeconds int64 `json:"request_timeout_seconds"`
	// OpenTimeoutSeconds is how long the breaker stays open.
	OpenTimeoutSeconds int64 `json:"open_timeout_seconds"`
	// FailureRateThreshold is the failure rate at which the breaker opens.
	FailureRateThreshold float64 `json:"failure_rate_threshold"`
	// MinimumRequestThreshold is the number of requests the window must hold
	// before the failure rate is acted on.
	MinimumRequestThreshold int64 `json:"minimum_request_threshold"`
	// TimeWindowSeconds is the length of the sliding window.
	TimeWindowSeconds int64 `json:"time_window_seconds"`
	// FailureCount is the number of failed requests in the window.
	FailureCount int64 `json:"failure_count"`
	// SuccessCount is the number of successful requests in the window.
	SuccessCount int64 `json:"success_count"`
	// TotalRequests is the number of completed requests in the window.
	TotalRequests int64 `json:"total_requests"`
}

// CircuitBreaker is a circuit breaker for one dependency. Create it with
// NewCircuitBreaker; the zero value is not usable.
//
// All methods are safe for concurrent use. Every field below except key is
// guarded by mu. Unexported methods documented as "mu must be held" rely on the
// caller for locking; everything else takes the lock itself. User supplied
// callbacks (the request function, the error predicate and both listeners) are
// never invoked while mu is held, so they may call back into the breaker.
type CircuitBreaker struct {
	stateTransitionTime        time.Time
	openedAt                   *time.Time
	onRequestComplete          func(success bool, duration time.Duration)
	statistics                 map[int64]*RequestStats
	onStateChange              func(from, to State)
	isFailure                  func(error) bool
	key                        string
	failureRateThreshold       float64
	openTimeout                time.Duration
	requestTimeout             time.Duration
	halfOpenRequestCount       int64
	thresholdRequestInHalfOpen int64
	targetRateInHalfOpen       float64
	halfOpenSuccessCount       int64
	halfOpenFailureCount       int64
	timeWindow                 time.Duration
	state                      State
	minimumRequestThreshold    int64
	// generation is incremented on every state transition. A request remembers
	// the generation it was admitted in, which lets a late result be told apart
	// from the results of the current half-open round.
	generation uint64
	mu         sync.Mutex
}

// admission is what a request carries from the moment the breaker lets it
// through until its outcome is recorded. It is filled in under mu, so the
// request itself runs without touching shared configuration.
type admission struct {
	isFailure      func(error) bool
	requestTimeout time.Duration
	generation     uint64
	// halfOpenProbe reports whether the request occupies one of the
	// thresholdRequestInHalfOpen probe slots of its generation.
	halfOpenProbe bool
}

var (
	// Global registry to manage multiple circuit breaker instances.
	circuitBreakerRegistry = make(map[string]*CircuitBreaker)

	registryMu sync.RWMutex

	// ErrCircuitBreakerOpen is returned by Execute and ExecuteWithTimeout when
	// the request was rejected and the wrapped function was not called: the
	// breaker is open, or it is half-open and all probe slots are taken.
	ErrCircuitBreakerOpen = errors.New("circuit breaker is open")

	// ErrCircuitBreakerTimeout is returned by Execute and ExecuteWithTimeout
	// when the request ended in a deadline: the wrapped function returned an
	// error wrapping context.DeadlineExceeded, or ExecuteWithTimeout stopped
	// waiting for it. The request counts as a failure.
	ErrCircuitBreakerTimeout = errors.New("circuit breaker request timeout")
)

// NewCircuitBreaker creates a new circuit breaker instance with the given key and options
// and registers it under key.
//
// Breakers are shared by key: if a breaker is already registered under key, that
// instance is returned as is and opts are ignored. This holds even when opts differ
// from the options the breaker was created with, and no error is reported. To
// reconfigure a registered breaker use its Set* methods, or call RemoveCircuitBreaker
// first to start over with a fresh instance.
func NewCircuitBreaker(key string, opts ...options.Option) *CircuitBreaker {
	registryMu.Lock()
	defer registryMu.Unlock()

	// Check if circuit breaker already exists for this key
	if cb, exists := circuitBreakerRegistry[key]; exists {
		return cb
	}

	// Create new circuit breaker with default values
	cb := &CircuitBreaker{
		key:                        key,
		state:                      StateClosed,
		stateTransitionTime:        time.Now(),
		statistics:                 make(map[int64]*RequestStats),
		timeWindow:                 1 * time.Minute,  // Default 1 minute
		minimumRequestThreshold:    10,               // Default 10 requests
		failureRateThreshold:       0.5,              // Default 50% failure rate
		openTimeout:                30 * time.Second, // Default 30 seconds
		requestTimeout:             5 * time.Second,  // Default 5 seconds
		halfOpenRequestCount:       0,
		thresholdRequestInHalfOpen: 10,  // Default 10 requests to evaluate
		targetRateInHalfOpen:       0.6, // Default 60% success rate
		halfOpenSuccessCount:       0,
		halfOpenFailureCount:       0,

		isFailure: defaultIsFailure, // Default error predicate
	}

	// Apply options
	for _, opt := range opts {
		opt(cb)
	}

	// Register the circuit breaker
	circuitBreakerRegistry[key] = cb

	return cb
}

// GetCircuitBreaker returns the breaker registered under key. The second result
// reports whether there is one; unlike NewCircuitBreaker it never creates it.
func GetCircuitBreaker(key string) (*CircuitBreaker, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	cb, exists := circuitBreakerRegistry[key]

	return cb, exists
}

// Execute runs fn if the breaker admits the request and records its outcome.
//
// It returns ErrCircuitBreakerOpen without calling fn when the breaker is open,
// or when it is half-open and all probe slots are taken. fn receives a context
// that is canceled after the request timeout; Execute itself waits for fn to
// return, so fn has to honor that context. If fn returns an error wrapping
// context.DeadlineExceeded the request counts as a failure and Execute returns
// ErrCircuitBreakerTimeout. Any other error is passed to the error predicate
// and returned unchanged.
//
// If fn or the error predicate panics, the request is recorded as a failure,
// exactly like a returned error, and the panic continues to the caller with its
// original value.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	// Check if we can proceed with the request
	adm, ok := cb.canProceed()
	if !ok {
		return ErrCircuitBreakerOpen
	}

	// Execute the function and measure duration
	start := time.Now()

	// A request that does not return normally, because fn or the error predicate
	// panicked, is a failed request. Record it while the panic unwinds; the panic
	// is not recovered here, so the caller sees it unchanged.
	completed := false

	defer func() {
		if !completed {
			cb.completeRequest(adm, false, time.Since(start))
		}
	}()

	// Create context with timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, adm.requestTimeout)
	defer cancel()

	err := fn(ctxWithTimeout)
	duration := time.Since(start)

	// Handle timeout case
	if errors.Is(err, context.DeadlineExceeded) {
		completed = true
		cb.completeRequest(adm, false, duration)

		return ErrCircuitBreakerTimeout
	}

	// Determine if this is a failure based on custom predicate
	isSuccess := err == nil || !adm.isFailure(err)

	completed = true
	cb.completeRequest(adm, isSuccess, duration)

	return err
}

// ExecuteWithTimeout is like Execute, but does not rely on fn to honor its
// context: fn runs in its own goroutine and ExecuteWithTimeout returns
// ErrCircuitBreakerTimeout as soon as the request timeout expires, recording a
// failure. In that case fn keeps running in the background until it returns and
// its result is discarded.
//
// If fn or the error predicate panics, the request is recorded as a failure and
// the panic is raised again in the caller's goroutine with its original value.
// A panic in fn after ExecuteWithTimeout has already returned on timeout has no
// caller left to go to; it is not swallowed and terminates the program like any
// other unrecovered panic.
func (cb *CircuitBreaker) ExecuteWithTimeout(ctx context.Context, fn func(context.Context) error) error {
	// Check if we can proceed with the request
	adm, ok := cb.canProceed()
	if !ok {
		return ErrCircuitBreakerOpen
	}

	// Execute the function with proper timeout handling
	start := time.Now()

	// A request that ends in a panic, whether from the error predicate or from
	// fn (re-raised below), is a failed request. Record it while the panic
	// unwinds this call; the caller sees the panic unchanged.
	completed := false

	defer func() {
		if !completed {
			cb.completeRequest(adm, false, time.Since(start))
		}
	}()

	// Create context with timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, adm.requestTimeout)
	defer cancel()

	resultChan := make(chan error, 1)
	done := make(chan struct{})

	// panicChan carries a panic out of fn's goroutine. It is unbuffered, so a
	// value is handed over only while this call is still waiting for it;
	// callerGone tells the goroutine that it no longer is.
	panicChan := make(chan any)
	callerGone := make(chan struct{})

	defer close(callerGone)

	// Execute function in goroutine with proper cleanup
	go func() {
		defer close(done) // Signal that goroutine is finishing

		defer func() {
			r := recover()
			if r == nil {
				return
			}

			select {
			case panicChan <- r:
			case <-callerGone:
				// Nobody is left to receive the panic; do not swallow it.
				panic(r)
			}
		}()

		select {
		case resultChan <- fn(ctxWithTimeout):
		case <-ctxWithTimeout.Done():
		}
	}()

	var (
		err      error
		duration time.Duration
	)

	// Wait for either completion or timeout
	select {
	case res := <-resultChan:
		duration = time.Since(start)
		err = res

		// Handle timeout case that might be returned by the function
		if errors.Is(err, context.DeadlineExceeded) {
			completed = true
			cb.completeRequest(adm, false, duration)

			return ErrCircuitBreakerTimeout
		}
	case r := <-panicChan:
		// fn panicked: continue the panic here, in the caller's goroutine.
		// The deferred function above records the failed request.
		panic(r)
	case <-ctxWithTimeout.Done():
		duration = time.Since(start)

		// Wait for goroutine cleanup with a reasonable timeout
		go func() {
			<-done // Wait for goroutine to finish
		}()

		// The context ends with DeadlineExceeded when the request timeout or a
		// deadline of the caller ran out. Canceled can only come from the
		// caller: that is not a timeout, so it is passed on as it is and left
		// to the error predicate below, the way Execute treats it.
		if cause := ctxWithTimeout.Err(); errors.Is(cause, context.Canceled) {
			err = cause

			break
		}

		completed = true
		cb.completeRequest(adm, false, duration)

		return ErrCircuitBreakerTimeout
	}

	// Determine if this is a failure based on custom predicate
	isSuccess := err == nil || !adm.isFailure(err)

	completed = true
	cb.completeRequest(adm, isSuccess, duration)

	return err
}

// canProceed decides whether a request may run in the current state and, if so,
// returns its admission. It is the only place where OPEN turns into HALF_OPEN.
//
// The decision and the bookkeeping happen in one critical section, so callers
// racing through an expired open timeout see a single transition and share one
// set of probe slots. In half-open a slot is taken when a probe is admitted,
// not when it completes: at most thresholdRequestInHalfOpen probes are ever in
// flight, however many callers arrive at once.
func (cb *CircuitBreaker) canProceed() (admission, bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Check if we should transition to half-open
	if cb.state == StateOpen && time.Since(cb.stateTransitionTime) >= cb.openTimeout {
		cb.setState(StateHalfOpen)
	}

	adm := admission{
		isFailure:      cb.isFailure,
		requestTimeout: cb.requestTimeout,
		generation:     cb.generation,
	}

	switch cb.state {
	case StateClosed:
		return adm, true
	case StateHalfOpen:
		// Allow requests up to the threshold for evaluation
		if cb.halfOpenRequestCount >= cb.thresholdRequestInHalfOpen {
			return admission{}, false
		}

		cb.halfOpenRequestCount++
		adm.halfOpenProbe = true

		return adm, true
	default:
		return admission{}, false
	}
}

// completeRequest records the outcome of an admitted request, applies any state
// change that follows from it and notifies the request complete listener.
//
// Statistics and state are updated in a single critical section, so no other
// request can observe the outcome half applied.
func (cb *CircuitBreaker) completeRequest(adm admission, success bool, duration time.Duration) {
	cb.mu.Lock()

	cb.recordRequest(success)

	// The generation changes on every transition, so a matching generation means
	// the breaker is still in the half-open round this probe was admitted in.
	// Results of requests admitted earlier are left out of the evaluation.
	if adm.halfOpenProbe && adm.generation == cb.generation {
		cb.updateStateAfterRequest(success)
	}

	listener := cb.onRequestComplete

	cb.mu.Unlock()

	triggerRequestCompleteEvent(listener, success, duration)
}

// recordRequest records the outcome of a request in the statistics and, while
// the breaker is closed, opens it if the failure rate calls for it.
// mu must be held.
func (cb *CircuitBreaker) recordRequest(success bool) {
	now := time.Now().Unix()

	if cb.statistics[now] == nil {
		cb.statistics[now] = &RequestStats{}
	}

	cb.statistics[now].RequestCount++
	if success {
		cb.statistics[now].SuccessCount++
	} else {
		cb.statistics[now].FailureCount++
	}

	cb.cleanOldStatistics()

	if cb.state == StateClosed {
		cb.checkAndUpdateState()
	}
}

// updateStateAfterRequest counts a completed probe of the current half-open
// round and, once thresholdRequestInHalfOpen probes have completed, closes or
// reopens the breaker depending on their success rate. mu must be held.
func (cb *CircuitBreaker) updateStateAfterRequest(success bool) {
	if cb.state != StateHalfOpen {
		return
	}

	if success {
		cb.halfOpenSuccessCount++
	} else {
		cb.halfOpenFailureCount++
	}

	totalHalfOpenRequests := cb.halfOpenSuccessCount + cb.halfOpenFailureCount
	if totalHalfOpenRequests < cb.thresholdRequestInHalfOpen {
		return
	}

	successRate := float64(cb.halfOpenSuccessCount) / float64(totalHalfOpenRequests)

	if successRate >= cb.targetRateInHalfOpen {
		// Success rate meets target, close the circuit
		cb.setState(StateClosed)
	} else {
		// Success rate below target, open the circuit
		cb.setState(StateOpen)
	}

	cb.resetHalfOpenCounters()
}

// resetHalfOpenCounters resets the half-open state counters. mu must be held.
func (cb *CircuitBreaker) resetHalfOpenCounters() {
	cb.halfOpenRequestCount = 0
	cb.halfOpenSuccessCount = 0
	cb.halfOpenFailureCount = 0
}

// checkAndUpdateState checks failure rate and updates state if necessary.
// mu must be held.
func (cb *CircuitBreaker) checkAndUpdateState() {
	totalRequests, totalFailures := cb.getStatisticsInWindow()

	if totalRequests < cb.minimumRequestThreshold {
		return
	}

	failureRate := float64(totalFailures) / float64(totalRequests)

	if failureRate >= cb.failureRateThreshold {
		cb.setState(StateOpen)
	}
}

// getStatisticsInWindow calculates total requests and failures within the time window.
// mu must be held.
func (cb *CircuitBreaker) getStatisticsInWindow() (totalRequests, totalFailures int64) {
	now := time.Now().Unix()
	windowStart := now - int64(cb.timeWindow.Seconds())

	for timestamp, stats := range cb.statistics {
		if timestamp >= windowStart {
			totalRequests += stats.RequestCount
			totalFailures += stats.FailureCount
		}
	}

	return totalRequests, totalFailures
}

// cleanOldStatistics removes statistics older than the time window. mu must be held.
func (cb *CircuitBreaker) cleanOldStatistics() {
	now := time.Now().Unix()
	windowStart := now - int64(cb.timeWindow.Seconds())

	for timestamp := range cb.statistics {
		if timestamp < windowStart {
			delete(cb.statistics, timestamp)
		}
	}
}

// setState changes the circuit breaker state and triggers events. mu must be held.
//
// The state change listener runs in its own goroutine, so it is not covered by
// the caller's lock and may call back into the breaker. Listener calls for
// successive transitions are not ordered relative to each other.
func (cb *CircuitBreaker) setState(newState State) {
	oldState := cb.state
	cb.state = newState
	cb.stateTransitionTime = time.Now()
	cb.generation++

	switch newState {
	case StateOpen:
		now := time.Now()
		cb.openedAt = &now
	case StateClosed:
		cb.openedAt = nil
		// A closed breaker judges the dependency by what happens from now on.
		// Without a fresh window the probes of the round that just closed it,
		// failed ones included, would count towards opening it again.
		cb.statistics = make(map[int64]*RequestStats)
	case StateHalfOpen:
		cb.statistics = make(map[int64]*RequestStats)
		cb.resetHalfOpenCounters()
	}

	if cb.onStateChange != nil {
		go cb.onStateChange(oldState, newState)
	}
}

// triggerRequestCompleteEvent triggers the request completion event.
// It must be called without mu held.
func triggerRequestCompleteEvent(listener func(success bool, duration time.Duration), success bool, duration time.Duration) {
	if listener != nil {
		// Call event handler without holding the lock to prevent deadlocks
		go listener(success, duration)
	}
}

// GetState returns the current state of the circuit breaker.
//
// The breaker leaves StateOpen when a request arrives after the open timeout,
// not when the timeout expires, so GetState keeps returning StateOpen for an
// idle breaker.
func (cb *CircuitBreaker) GetState() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	return cb.state
}

// GetStatistics returns the request counters of the current time window:
// completed requests, how many of them succeeded and failed, and the failure
// rate between 0 and 1. Rejected requests are not counted. The window starts
// empty every time the breaker turns half-open.
func (cb *CircuitBreaker) GetStatistics() (totalRequests, successCount, failureCount int64, failureRate float64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	totalRequests, failureCount = cb.getStatisticsInWindow()
	successCount = totalRequests - failureCount

	if totalRequests > 0 {
		failureRate = float64(failureCount) / float64(totalRequests)
	}

	return totalRequests, successCount, failureCount, failureRate
}

// GetDetailedStatistics returns a snapshot of the breaker's state, configuration
// and counters. See DetailedStatistics for the fields.
func (cb *CircuitBreaker) GetDetailedStatistics() DetailedStatistics {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	totalRequests, failureCount := cb.getStatisticsInWindow()
	successCount := totalRequests - failureCount
	failureRate := 0.0
	if totalRequests > 0 {
		failureRate = float64(failureCount) / float64(totalRequests)
	}

	stats := DetailedStatistics{
		Key:                        cb.key,
		State:                      cb.state.String(),
		TotalRequests:              totalRequests,
		SuccessCount:               successCount,
		FailureCount:               failureCount,
		FailureRate:                failureRate,
		TimeWindowSeconds:          int64(cb.timeWindow.Seconds()),
		MinimumRequestThreshold:    cb.minimumRequestThreshold,
		FailureRateThreshold:       cb.failureRateThreshold,
		OpenTimeoutSeconds:         int64(cb.openTimeout.Seconds()),
		RequestTimeoutSeconds:      int64(cb.requestTimeout.Seconds()),
		ThresholdRequestInHalfOpen: cb.thresholdRequestInHalfOpen,
		TargetRateInHalfOpen:       cb.targetRateInHalfOpen,
	}

	// Add open-specific information
	if cb.state == StateOpen && cb.openedAt != nil {
		openedAtStr := cb.openedAt.Format(time.RFC3339)
		stats.OpenedAt = &openedAtStr
		nextHalfOpenTime := cb.openedAt.Add(cb.openTimeout)
		nextHalfOpenStr := nextHalfOpenTime.Format(time.RFC3339)
		stats.NextHalfOpenAt = &nextHalfOpenStr

		timeUntilHalfOpen := time.Until(nextHalfOpenTime)
		if timeUntilHalfOpen > 0 {
			timeUntilHalfOpenSeconds := int64(timeUntilHalfOpen.Seconds())
			stats.TimeUntilHalfOpenSeconds = &timeUntilHalfOpenSeconds
		} else {
			stats.TimeUntilHalfOpenSeconds = nil
		}
	}

	// Add half-open specific information
	if cb.state == StateHalfOpen {
		// Copy the counters: pointers into cb would be read by the caller
		// after the lock is released.
		halfOpenRequestCount := cb.halfOpenRequestCount
		halfOpenSuccessCount := cb.halfOpenSuccessCount
		halfOpenFailureCount := cb.halfOpenFailureCount

		stats.HalfOpenRequestCount = &halfOpenRequestCount
		remainingTestRequests := cb.thresholdRequestInHalfOpen - halfOpenRequestCount
		stats.RemainingTestRequests = &remainingTestRequests

		// New half-open statistics
		stats.HalfOpenSuccessCount = &halfOpenSuccessCount
		stats.HalfOpenFailureCount = &halfOpenFailureCount

		totalHalfOpenRequests := halfOpenSuccessCount + halfOpenFailureCount
		if totalHalfOpenRequests > 0 {
			successRate := float64(halfOpenSuccessCount) / float64(totalHalfOpenRequests)
			stats.HalfOpenSuccessRate = &successRate
		}
	}

	return stats
}

// GetKey returns the key this circuit breaker was created with.
func (cb *CircuitBreaker) GetKey() string {
	return cb.key
}

// String returns a one-line summary of the breaker for logs: key, state and
// the counters of the current time window.
func (cb *CircuitBreaker) String() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	totalRequests, totalFailures := cb.getStatisticsInWindow()
	failureRate := 0.0
	if totalRequests > 0 {
		failureRate = float64(totalFailures) / float64(totalRequests)
	}

	return fmt.Sprintf("CircuitBreaker{key=%s, state=%s, requests=%d, failures=%d, failureRate=%.2f%%}",
		cb.key, cb.state.String(), totalRequests, totalFailures, failureRate*100)
}

// defaultIsFailure is the default error predicate that considers all non-nil errors as failures.
func defaultIsFailure(err error) bool {
	return err != nil
}

// RemoveCircuitBreaker removes the breaker registered under key from the
// registry, if there is one. The breaker itself keeps working for whoever still
// holds it; the next NewCircuitBreaker call with key creates a new one.
func RemoveCircuitBreaker(key string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(circuitBreakerRegistry, key)
}

// ListCircuitBreakers returns the keys of all registered circuit breakers, in
// no particular order.
func ListCircuitBreakers() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	keys := make([]string, 0, len(circuitBreakerRegistry))
	for key := range circuitBreakerRegistry {
		keys = append(keys, key)
	}

	return keys
}

// The setters below implement options.CircuitBreakerConfig. They may be called
// at any time, also while requests are in flight. They store the value as it
// is: the range checks are done by the With functions of the options package,
// not here.

// SetTimeWindow sets the time window for failure analysis. The window is
// counted in whole seconds. A duration that is not positive is ignored.
func (cb *CircuitBreaker) SetTimeWindow(duration time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if duration > 0 {
		cb.timeWindow = duration
	}
}

// SetMinimumRequestThreshold sets the number of requests the time window must
// hold before the failure rate can open the breaker. A threshold that is not
// positive is ignored.
func (cb *CircuitBreaker) SetMinimumRequestThreshold(threshold int64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if threshold > 0 {
		cb.minimumRequestThreshold = threshold
	}
}

// SetFailureRateThreshold sets the failure rate, between 0 and 1, at which the
// closed breaker opens. A rate outside that range is ignored.
func (cb *CircuitBreaker) SetFailureRateThreshold(threshold float64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if threshold >= 0 && threshold <= 1 {
		cb.failureRateThreshold = threshold
	}
}

// SetOpenTimeout sets how long the circuit breaker stays open before it lets
// probes through. It applies to a breaker that is open right now as well. A
// timeout that is not positive is ignored.
func (cb *CircuitBreaker) SetOpenTimeout(timeout time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if timeout > 0 {
		cb.openTimeout = timeout
	}
}

// SetRequestTimeout sets the maximum duration for individual requests. It
// applies to requests admitted from now on. A timeout that is not positive is
// ignored.
func (cb *CircuitBreaker) SetRequestTimeout(timeout time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if timeout > 0 {
		cb.requestTimeout = timeout
	}
}

// SetThresholdRequestInHalfOpen sets the number of probe requests admitted and
// evaluated per half-open round. A threshold that is not positive is ignored.
func (cb *CircuitBreaker) SetThresholdRequestInHalfOpen(threshold int64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if threshold > 0 {
		cb.thresholdRequestInHalfOpen = threshold
	}
}

// SetTargetRateInHalfOpen sets the share of successful probes, between 0 and 1,
// required to close the circuit after a half-open round. A rate outside that
// range is ignored.
func (cb *CircuitBreaker) SetTargetRateInHalfOpen(rate float64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if rate >= 0 && rate <= 1 {
		cb.targetRateInHalfOpen = rate
	}
}

// SetCustomErrorPredicate sets the function that decides whether an error
// returned by the wrapped function counts as a failure. It is not asked about
// nil errors or timeouts. A nil predicate is ignored.
func (cb *CircuitBreaker) SetCustomErrorPredicate(predicate func(error) bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if predicate != nil {
		cb.isFailure = predicate
	}
}

// SetStateChangeListener sets a callback for state changes. The callback runs
// in its own goroutine for every transition.
//
// listener must be a func(from, to State), or a value of a defined type with
// that signature. A nil interface value is ignored and keeps the listener set
// earlier. Any other value is a programming error and panics, because a
// listener that is dropped without a word leaves a breaker running without the
// listener its owner thinks it has.
func (cb *CircuitBreaker) SetStateChangeListener(listener any) {
	if listener == nil {
		return
	}

	l, ok := asStateChangeListener(listener)
	if !ok {
		panic(fmt.Sprintf("circuitbreaker: state change listener must be a func(from, to circuitbreaker.State), got %T", listener))
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.onStateChange = l
}

// asStateChangeListener returns listener as a func(from, to State). Besides the
// plain func type it accepts a defined type with the same signature.
func asStateChangeListener(listener any) (func(from, to State), bool) {
	if l, ok := listener.(func(from, to State)); ok {
		return l, true
	}

	want := reflect.TypeFor[func(from, to State)]()

	v := reflect.ValueOf(listener)
	if v.Kind() != reflect.Func || !v.Type().ConvertibleTo(want) {
		return nil, false
	}

	l, ok := v.Convert(want).Interface().(func(from, to State))

	return l, ok
}

// SetRequestCompleteListener sets a callback for request completion. The
// callback runs in its own goroutine for every completed request; a request
// that timed out or panicked is reported with success=false.
func (cb *CircuitBreaker) SetRequestCompleteListener(listener func(success bool, duration time.Duration)) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.onRequestComplete = listener
}
