package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/anilonayy/circuit-breaker/options"
)

// State represents the current state of the Circuit Breaker.
type State int

const (
	// StateClosed - Normal operating state, requests pass through.
	StateClosed State = iota
	// StateOpen - Circuit is open, all requests are blocked (fail-fast).
	StateOpen
	// StateHalfOpen - Limited test requests are allowed to check service health.
	StateHalfOpen
)

// String returns the string representation of the state.
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

// RequestStats holds statistics for requests within a specific second.
type RequestStats struct {
	RequestCount int64 // Total number of requests in that second
	SuccessCount int64 // Number of successful requests in that second
	FailureCount int64 // Number of failed requests in that second
}

// DetailedStatistics holds detailed statistics for a circuit breaker.
type DetailedStatistics struct {
	OpenedAt                   *string  `json:"opened_at,omitempty"`
	HalfOpenSuccessRate        *float64 `json:"half_open_success_rate,omitempty"`
	HalfOpenFailureCount       *int64   `json:"half_open_failure_count,omitempty"`
	HalfOpenSuccessCount       *int64   `json:"half_open_success_count,omitempty"`
	RemainingTestRequests      *int64   `json:"remaining_test_requests,omitempty"`
	HalfOpenRequestCount       *int64   `json:"half_open_request_count,omitempty"`
	TimeUntilHalfOpenSeconds   *int64   `json:"time_until_half_open_seconds,omitempty"`
	NextHalfOpenAt             *string  `json:"next_half_open_at,omitempty"`
	State                      string   `json:"state"`
	Key                        string   `json:"key"`
	FailureRate                float64  `json:"failure_rate"`
	ThresholdRequestInHalfOpen int64    `json:"threshold_request_in_half_open"`
	TargetRateInHalfOpen       float64  `json:"target_rate_in_half_open"`
	RequestTimeoutSeconds      int64    `json:"request_timeout_seconds"`
	OpenTimeoutSeconds         int64    `json:"open_timeout_seconds"`
	FailureRateThreshold       float64  `json:"failure_rate_threshold"`
	MinimumRequestThreshold    int64    `json:"minimum_request_threshold"`
	TimeWindowSeconds          int64    `json:"time_window_seconds"`
	FailureCount               int64    `json:"failure_count"`
	SuccessCount               int64    `json:"success_count"`
	TotalRequests              int64    `json:"total_requests"`
}

// CircuitBreaker represents a circuit breaker instance.
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
	mu                         sync.Mutex
}

var (
	// Global registry to manage multiple circuit breaker instances.
	circuitBreakerRegistry = make(map[string]*CircuitBreaker)

	registryMu sync.RWMutex

	ErrCircuitBreakerOpen    = errors.New("circuit breaker is open")
	ErrCircuitBreakerTimeout = errors.New("circuit breaker request timeout")
)

// NewCircuitBreaker creates a new circuit breaker instance with the given key and options.
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

// GetCircuitBreaker retrieves an existing circuit breaker by key.
func GetCircuitBreaker(key string) (*CircuitBreaker, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	cb, exists := circuitBreakerRegistry[key]

	return cb, exists
}

func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	// Check if we can proceed with the request
	if !cb.canProceed() {
		return ErrCircuitBreakerOpen
	}

	// Create context with timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, cb.requestTimeout)
	defer cancel()

	// Execute the function and measure duration
	start := time.Now()
	err := fn(ctxWithTimeout)
	duration := time.Since(start)

	// Handle timeout case
	if errors.Is(err, context.DeadlineExceeded) {
		cb.recordRequest(false)
		cb.triggerRequestCompleteEvent(false, duration)

		return ErrCircuitBreakerTimeout
	}

	// Determine if this is a failure based on custom predicate
	isSuccess := err == nil || !cb.isFailure(err)

	// Record the request
	cb.recordRequest(isSuccess)

	// Trigger event
	cb.triggerRequestCompleteEvent(isSuccess, duration)

	// Update state based on result (especially important for half-open state)
	cb.updateStateAfterRequest(isSuccess)

	return err
}

// Execute wraps a function call with circuit breaker logic.
func (cb *CircuitBreaker) ExecuteWithTimeout(ctx context.Context, fn func(context.Context) error) error {
	// Check if we can proceed with the request
	if !cb.canProceed() {
		return ErrCircuitBreakerOpen
	}

	// Create context with timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, cb.requestTimeout)
	defer cancel()

	// Execute the function with proper timeout handling
	start := time.Now()

	resultChan := make(chan error, 1)
	done := make(chan struct{})

	// Execute function in goroutine with proper cleanup
	go func() {
		defer close(done) // Signal that goroutine is finishing

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
			cb.recordRequest(false)
			cb.triggerRequestCompleteEvent(false, duration)

			return ErrCircuitBreakerTimeout
		}
	case <-ctxWithTimeout.Done():
		// Timeout occurred - wait for goroutine to finish cleanup
		duration = time.Since(start)
		cb.recordRequest(false)
		cb.triggerRequestCompleteEvent(false, duration)

		// Wait for goroutine cleanup with a reasonable timeout
		go func() {
			<-done // Wait for goroutine to finish
		}()

		return ErrCircuitBreakerTimeout
	}

	// Determine if this is a failure based on custom predicate
	isSuccess := err == nil || !cb.isFailure(err)

	// Record the request
	cb.recordRequest(isSuccess)

	// Trigger event
	cb.triggerRequestCompleteEvent(isSuccess, duration)

	// Update state based on result (especially important for half-open state)
	cb.updateStateAfterRequest(isSuccess)

	return err
}

// canProceed determines if a request can proceed based on current state.
func (cb *CircuitBreaker) canProceed() bool {
	handleStateHalfOpen := func() bool {
		// Allow requests up to the threshold for evaluation
		totalHalfOpenRequests := cb.halfOpenSuccessCount + cb.halfOpenFailureCount
		if totalHalfOpenRequests < cb.thresholdRequestInHalfOpen {
			cb.halfOpenRequestCount++

			return true
		}

		return false
	}

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if we should transition to half-open
		if time.Since(cb.stateTransitionTime) >= cb.openTimeout {
			cb.setState(StateHalfOpen)

			return handleStateHalfOpen()
		}

		return false
	case StateHalfOpen:
		return handleStateHalfOpen()
	default:
		return false
	}
}

// recordRequest records the outcome of a request and updates statistics.
func (cb *CircuitBreaker) recordRequest(success bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

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

// updateStateAfterRequest updates the circuit breaker state after a request in half-open state.
func (cb *CircuitBreaker) updateStateAfterRequest(success bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateHalfOpen {
		if success {
			cb.halfOpenSuccessCount++
		} else {
			cb.halfOpenFailureCount++
		}

		totalHalfOpenRequests := cb.halfOpenSuccessCount + cb.halfOpenFailureCount

		if totalHalfOpenRequests >= cb.thresholdRequestInHalfOpen {
			successRate := float64(cb.halfOpenSuccessCount) / float64(totalHalfOpenRequests)

			if successRate >= cb.targetRateInHalfOpen {
				// Success rate meets target, close the circuit
				cb.setState(StateClosed)
				cb.resetHalfOpenCounters()
			} else {
				// Success rate below target, open the circuit
				cb.setState(StateOpen)
				cb.resetHalfOpenCounters()
			}
		} else if totalHalfOpenRequests >= cb.thresholdRequestInHalfOpen &&
			cb.halfOpenFailureCount >= int64(float64(totalHalfOpenRequests)*cb.failureRateThreshold) {
			cb.setState(StateOpen)
			cb.resetHalfOpenCounters()
		}
	}
}

// resetHalfOpenCounters resets the half-open state counters.
func (cb *CircuitBreaker) resetHalfOpenCounters() {
	cb.halfOpenRequestCount = 0
	cb.halfOpenSuccessCount = 0
	cb.halfOpenFailureCount = 0
}

// checkAndUpdateState checks failure rate and updates state if necessary.
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

// cleanOldStatistics removes statistics older than the time window.
func (cb *CircuitBreaker) cleanOldStatistics() {
	now := time.Now().Unix()
	windowStart := now - int64(cb.timeWindow.Seconds())

	for timestamp := range cb.statistics {
		if timestamp < windowStart {
			delete(cb.statistics, timestamp)
		}
	}
}

func (cb *CircuitBreaker) cleanAllStatistics() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.statistics = make(map[int64]*RequestStats)
}

// setState changes the circuit breaker state and triggers events.
func (cb *CircuitBreaker) setState(newState State) {
	oldState := cb.state
	cb.state = newState
	cb.stateTransitionTime = time.Now()

	switch newState {
	case StateOpen:
		now := time.Now()
		cb.openedAt = &now
	case StateClosed:
		cb.openedAt = nil
	case StateHalfOpen:
		cb.cleanAllStatistics()
		cb.resetHalfOpenCounters()
	}

	if cb.onStateChange != nil {
		go cb.onStateChange(oldState, newState)
	}
}

// triggerRequestCompleteEvent triggers the request completion event.
func (cb *CircuitBreaker) triggerRequestCompleteEvent(success bool, duration time.Duration) {
	if cb.onRequestComplete != nil {
		// Call event handler without holding the lock to prevent deadlocks
		go cb.onRequestComplete(success, duration)
	}
}

// GetState returns the current state of the circuit breaker.
func (cb *CircuitBreaker) GetState() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	return cb.state
}

// GetStatistics returns current statistics within the time window.
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

// GetDetailedStatistics returns detailed statistics including open time and next half-open time.
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
		stats.HalfOpenRequestCount = &cb.halfOpenRequestCount
		remainingTestRequests := cb.thresholdRequestInHalfOpen - cb.halfOpenRequestCount
		stats.RemainingTestRequests = &remainingTestRequests

		// New half-open statistics
		stats.HalfOpenSuccessCount = &cb.halfOpenSuccessCount
		stats.HalfOpenFailureCount = &cb.halfOpenFailureCount

		totalHalfOpenRequests := cb.halfOpenSuccessCount + cb.halfOpenFailureCount
		if totalHalfOpenRequests > 0 {
			successRate := float64(cb.halfOpenSuccessCount) / float64(totalHalfOpenRequests)
			stats.HalfOpenSuccessRate = &successRate
		}
	}

	return stats
}

// GetKey returns the unique key of this circuit breaker.
func (cb *CircuitBreaker) GetKey() string {
	return cb.key
}

// String returns a string representation of the circuit breaker.
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

// RemoveCircuitBreaker removes a circuit breaker from the registry.
func RemoveCircuitBreaker(key string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(circuitBreakerRegistry, key)
}

// ListCircuitBreakers returns all registered circuit breaker keys.
func ListCircuitBreakers() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	keys := make([]string, 0, len(circuitBreakerRegistry))
	for key := range circuitBreakerRegistry {
		keys = append(keys, key)
	}

	return keys
}

// Setter methods to implement the CircuitBreakerConfig interface

// SetTimeWindow sets the time window for failure analysis.
func (cb *CircuitBreaker) SetTimeWindow(duration time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.timeWindow = duration
}

// SetMinimumRequestThreshold sets the minimum number of requests required.
func (cb *CircuitBreaker) SetMinimumRequestThreshold(threshold int64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.minimumRequestThreshold = threshold
}

// SetFailureRateThreshold sets the failure rate threshold.
func (cb *CircuitBreaker) SetFailureRateThreshold(threshold float64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureRateThreshold = threshold
}

// SetOpenTimeout sets how long the circuit breaker stays open.
func (cb *CircuitBreaker) SetOpenTimeout(timeout time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.openTimeout = timeout
}

// SetRequestTimeout sets the maximum duration for individual requests.
func (cb *CircuitBreaker) SetRequestTimeout(timeout time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.requestTimeout = timeout
}

// SetThresholdRequestInHalfOpen sets the threshold number of requests to evaluate in half-open state.
func (cb *CircuitBreaker) SetThresholdRequestInHalfOpen(threshold int64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.thresholdRequestInHalfOpen = threshold
}

// SetTargetRateInHalfOpen sets the target success rate required to close the circuit in half-open state.
func (cb *CircuitBreaker) SetTargetRateInHalfOpen(rate float64) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.targetRateInHalfOpen = rate
}

// SetCustomErrorPredicate sets a custom function to determine failures.
func (cb *CircuitBreaker) SetCustomErrorPredicate(predicate func(error) bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.isFailure = predicate
}

// SetStateChangeListener sets a callback for state changes.
func (cb *CircuitBreaker) SetStateChangeListener(listener any) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if l, ok := listener.(func(from, to State)); ok {
		cb.onStateChange = l
	}
}

// SetRequestCompleteListener sets a callback for request completion.
func (cb *CircuitBreaker) SetRequestCompleteListener(listener func(success bool, duration time.Duration)) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.onRequestComplete = listener
}
