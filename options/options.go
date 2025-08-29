package options

import (
	"time"
)

// CircuitBreakerConfig represents the configurable fields of a circuit breaker.
type CircuitBreakerConfig interface {
	SetTimeWindow(duration time.Duration)
	SetMinimumRequestThreshold(threshold int64)
	SetFailureRateThreshold(threshold float64)
	SetOpenTimeout(timeout time.Duration)
	SetRequestTimeout(timeout time.Duration)
	SetThresholdRequestInHalfOpen(threshold int64)
	SetTargetRateInHalfOpen(rate float64)
	SetCustomErrorPredicate(predicate func(error) bool)
	SetStateChangeListener(listener any)
	SetRequestCompleteListener(listener func(success bool, duration time.Duration))
}

// Option represents a functional option for configuring the Circuit Breaker.
type Option func(CircuitBreakerConfig)

// WithTimeWindow sets the time window for failure analysis
// The time window must be in multiples of minutes (e.g., 1*time.Minute, 5*time.Minute)
// Default: 1 minute.
func WithTimeWindow(duration time.Duration) Option {
	return func(cb CircuitBreakerConfig) {
		if duration > 0 {
			cb.SetTimeWindow(duration)
		}
	}
}

// WithMinimumRequestThreshold sets the minimum number of requests required
// before the circuit breaker starts analyzing failure rates
// Default: 10 requests.
func WithMinimumRequestThreshold(threshold int64) Option {
	return func(cb CircuitBreakerConfig) {
		if threshold > 0 {
			cb.SetMinimumRequestThreshold(threshold)
		}
	}
}

// WithFailureRateThreshold sets the failure rate threshold (0.0 to 1.0)
// that triggers the circuit breaker to open
// Example: 0.5 means 50% failure rate
// Default: 0.5 (50%).
func WithFailureRateThreshold(threshold float64) Option {
	return func(cb CircuitBreakerConfig) {
		if threshold >= 0.0 && threshold <= 1.0 {
			cb.SetFailureRateThreshold(threshold)
		}
	}
}

// WithOpenTimeout sets how long the circuit breaker stays in the Open state
// before transitioning to Half-Open state
// Default: 30 seconds.
func WithOpenTimeout(timeout time.Duration) Option {
	return func(cb CircuitBreakerConfig) {
		if timeout > 0 {
			cb.SetOpenTimeout(timeout)
		}
	}
}

// WithRequestTimeout sets the maximum duration for individual requests
// Requests exceeding this timeout will be canceled and counted as failures
// Default: 5 seconds.
func WithRequestTimeout(timeout time.Duration) Option {
	return func(cb CircuitBreakerConfig) {
		if timeout > 0 {
			cb.SetRequestTimeout(timeout)
		}
	}
}

// WithThresholdRequestInHalfOpen sets the threshold number of requests to evaluate
// in half-open state before making a transition decision
// Default: 10 requests.
func WithThresholdRequestInHalfOpen(threshold int64) Option {
	return func(cb CircuitBreakerConfig) {
		if threshold > 0 {
			cb.SetThresholdRequestInHalfOpen(threshold)
		}
	}
}

// WithTargetRateInHalfOpen sets the target success rate (0.0 to 1.0) required
// to close the circuit when in half-open state
// Example: 0.6 means 60% success rate is required
// Default: 0.6 (60%).
func WithTargetRateInHalfOpen(rate float64) Option {
	return func(cb CircuitBreakerConfig) {
		if rate >= 0.0 && rate <= 1.0 {
			cb.SetTargetRateInHalfOpen(rate)
		}
	}
}

// WithCustomErrorPredicate sets a custom function to determine
// which errors should be considered as failures
// The function should return true if the error represents a failure
// Default: all non-nil errors are considered failures
//
// Example usage:
//
//	WithCustomErrorPredicate(func(err error) bool {
//	    // Only consider network errors as failures
//	    var netErr net.Error
//	    return errors.As(err, &netErr)
//	})
func WithCustomErrorPredicate(predicate func(error) bool) Option {
	return func(cb CircuitBreakerConfig) {
		if predicate != nil {
			cb.SetCustomErrorPredicate(predicate)
		}
	}
}

// WithStateChangeListener sets a callback function that gets called
// whenever the circuit breaker changes state
// The callback receives the old state and new state as parameters
//
// Example usage:
//
//	WithStateChangeListener(func(from, to State) {
//	    log.Printf("Circuit breaker state changed from %s to %s", from, to)
//	})
func WithStateChangeListener(listener any) Option {
	return func(cb CircuitBreakerConfig) {
		cb.SetStateChangeListener(listener)
	}
}

func WithRequestCompleteListener(listener func(success bool, duration time.Duration)) Option {
	return func(cb CircuitBreakerConfig) {
		cb.SetRequestCompleteListener(listener)
	}
}

// Convenience functions for common configurations

// WithDefaults returns the default configuration options
// This is useful for explicitly setting defaults or as a base for customization.
func WithDefaults() []Option {
	return []Option{
		WithTimeWindow(60 * time.Second),
		WithMinimumRequestThreshold(100),
		WithFailureRateThreshold(0.5),
		WithOpenTimeout(30 * time.Second),
		WithRequestTimeout(5 * time.Second),
		WithThresholdRequestInHalfOpen(10),
		WithTargetRateInHalfOpen(0.6),
	}
}

func WithTestDefaults() []Option {
	return []Option{
		WithTimeWindow(60 * time.Second),
		WithMinimumRequestThreshold(5),
		WithFailureRateThreshold(0.5),
		WithOpenTimeout(30 * time.Second),
		WithRequestTimeout(3 * time.Second),
		WithThresholdRequestInHalfOpen(10),
		WithTargetRateInHalfOpen(0.6),
	}
}

// WithAggressive returns options for an aggressive circuit breaker
// that opens quickly and stays open longer.
func WithAggressive() []Option {
	return []Option{
		WithTimeWindow(30 * time.Second),    // Shorter analysis window
		WithMinimumRequestThreshold(5),      // Lower threshold
		WithFailureRateThreshold(0.3),       // 30% failure rate
		WithOpenTimeout(2 * time.Minute),    // Stay open longer
		WithRequestTimeout(3 * time.Second), // Shorter request timeout
		WithThresholdRequestInHalfOpen(5),   // Fewer requests to evaluate
		WithTargetRateInHalfOpen(0.8),       // Higher success rate required
	}
}

// WithConservative returns options for a conservative circuit breaker
// that is more tolerant of failures and recovers quickly.
func WithConservative() []Option {
	return []Option{
		WithTimeWindow(2 * time.Minute),      // Longer analysis window
		WithMinimumRequestThreshold(20),      // Higher threshold
		WithFailureRateThreshold(0.7),        // 70% failure rate
		WithOpenTimeout(15 * time.Second),    // Recover quickly
		WithRequestTimeout(10 * time.Second), // Longer request timeout
		WithThresholdRequestInHalfOpen(15),   // More requests to evaluate
		WithTargetRateInHalfOpen(0.5),        // Lower success rate required
	}
}

// WithHTTPDefaults returns options optimized for HTTP requests.
func WithHTTPDefaults() []Option {
	return []Option{
		WithTimeWindow(1 * time.Minute),
		WithMinimumRequestThreshold(10),
		WithFailureRateThreshold(0.5),
		WithOpenTimeout(30 * time.Second),
		WithRequestTimeout(30 * time.Second), // Longer timeout for HTTP
		WithCustomErrorPredicate(func(err error) bool {
			// Consider timeouts and context cancellations as failures
			// but not other application-level errors unless specified
			return err != nil
		}),
	}
}

// WithDatabaseDefaults returns options optimized for database connections.
func WithDatabaseDefaults() []Option {
	return []Option{
		WithTimeWindow(2 * time.Minute),      // Longer window for DB
		WithMinimumRequestThreshold(15),      // Higher threshold
		WithFailureRateThreshold(0.6),        // More tolerant
		WithOpenTimeout(45 * time.Second),    // DB recovery time
		WithRequestTimeout(15 * time.Second), // DB query timeout
	}
}

// WithMicroserviceDefaults returns options optimized for microservice calls.
func WithMicroserviceDefaults() []Option {
	return []Option{
		WithTimeWindow(1 * time.Minute),
		WithMinimumRequestThreshold(8),
		WithFailureRateThreshold(0.4),       // More sensitive
		WithOpenTimeout(20 * time.Second),   // Quick recovery
		WithRequestTimeout(8 * time.Second), // Service call timeout
	}
}

// Builder pattern for more complex configurations

// ConfigBuilder provides a builder pattern for circuit breaker configuration.
type ConfigBuilder struct {
	options []Option
}

// NewConfigBuilder creates a new configuration builder.
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		options: make([]Option, 0),
	}
}

// TimeWindow adds a time window option to the builder.
func (b *ConfigBuilder) TimeWindow(seconds int64) *ConfigBuilder {
	b.options = append(b.options, WithTimeWindow(time.Duration(seconds)*time.Second))

	return b
}

// MinimumRequestThreshold adds a minimum request threshold option to the builder.
func (b *ConfigBuilder) MinimumRequestThreshold(threshold int64) *ConfigBuilder {
	b.options = append(b.options, WithMinimumRequestThreshold(threshold))

	return b
}

// FailureRateThreshold adds a failure rate threshold option to the builder.
func (b *ConfigBuilder) FailureRateThreshold(threshold float64) *ConfigBuilder {
	b.options = append(b.options, WithFailureRateThreshold(threshold))

	return b
}

// OpenTimeout adds an open timeout option to the builder.
func (b *ConfigBuilder) OpenTimeout(seconds int64) *ConfigBuilder {
	b.options = append(b.options, WithOpenTimeout(time.Duration(seconds)*time.Second))

	return b
}

// RequestTimeout adds a request timeout option to the builder.
func (b *ConfigBuilder) RequestTimeout(seconds int64) *ConfigBuilder {
	b.options = append(b.options, WithRequestTimeout(time.Duration(seconds)*time.Second))

	return b
}

// ThresholdRequestInHalfOpen adds a threshold request in half-open option to the builder.
func (b *ConfigBuilder) ThresholdRequestInHalfOpen(threshold int64) *ConfigBuilder {
	b.options = append(b.options, WithThresholdRequestInHalfOpen(threshold))

	return b
}

// TargetRateInHalfOpen adds a target rate in half-open option to the builder.
func (b *ConfigBuilder) TargetRateInHalfOpen(rate float64) *ConfigBuilder {
	b.options = append(b.options, WithTargetRateInHalfOpen(rate))

	return b
}

// CustomErrorPredicate adds a custom error predicate option to the builder.
func (b *ConfigBuilder) CustomErrorPredicate(predicate func(error) bool) *ConfigBuilder {
	b.options = append(b.options, WithCustomErrorPredicate(predicate))

	return b
}

// StateChangeListener adds a state change listener option to the builder.
func (b *ConfigBuilder) StateChangeListener(listener any) *ConfigBuilder {
	b.options = append(b.options, WithStateChangeListener(listener))

	return b
}

// RequestCompleteListener adds a request complete listener option to the builder.
func (b *ConfigBuilder) RequestCompleteListener(listener func(success bool, duration time.Duration)) *ConfigBuilder {
	b.options = append(b.options, WithRequestCompleteListener(listener))

	return b
}

// Build returns the configured options.
func (b *ConfigBuilder) Build() []Option {
	return b.options
}
