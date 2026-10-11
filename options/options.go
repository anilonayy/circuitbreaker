// Package options holds the functional options that configure a circuit
// breaker from github.com/anilonayy/circuitbreaker.
//
// Pass them to circuitbreaker.NewCircuitBreaker, one by one or as a preset such
// as WithHTTPDefaults, optionally followed by overrides:
//
//	opts := append(options.WithHTTPDefaults(), options.WithOpenTimeout(time.Minute))
//	cb := circuitbreaker.NewCircuitBreaker("search", opts...)
//
// Options are applied in order, so a later one overrides an earlier one.
//
// An option whose value is out of range (a duration or count that is not
// positive, a rate outside 0 to 1, a nil predicate) does nothing: the breaker
// keeps the value it had, and no error is reported.
package options

import (
	"time"
)

// CircuitBreakerConfig is the set of setters an Option works through. It is
// implemented by *circuitbreaker.CircuitBreaker; the interface exists so that
// this package does not have to import that one.
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

// Option configures one aspect of a circuit breaker. Options are created by the
// With functions of this package and applied by circuitbreaker.NewCircuitBreaker.
type Option func(CircuitBreakerConfig)

// WithTimeWindow sets the sliding time window over which the failure rate is
// calculated. Outcomes are kept in one-second buckets, so the window is counted
// in whole seconds and a fraction of a second is dropped.
// A duration that is not positive is ignored.
// Default: 1 minute.
func WithTimeWindow(duration time.Duration) Option {
	return func(cb CircuitBreakerConfig) {
		if duration > 0 {
			cb.SetTimeWindow(duration)
		}
	}
}

// WithMinimumRequestThreshold sets the minimum number of requests the time
// window must hold before the circuit breaker acts on the failure rate.
// A threshold that is not positive is ignored.
// Default: 10 requests.
func WithMinimumRequestThreshold(threshold int64) Option {
	return func(cb CircuitBreakerConfig) {
		if threshold > 0 {
			cb.SetMinimumRequestThreshold(threshold)
		}
	}
}

// WithFailureRateThreshold sets the failure rate threshold (0.0 to 1.0)
// that triggers the circuit breaker to open: it opens when the rate is equal to
// or above the threshold.
// Example: 0.5 means 50% failure rate.
// Both ends of the range are accepted. At 1.0 only a window in which every
// request failed opens the breaker; at 0.0 it opens as soon as the minimum
// number of requests is reached, failures or not.
// A threshold outside the range is ignored.
// Default: 0.5 (50%).
func WithFailureRateThreshold(threshold float64) Option {
	return func(cb CircuitBreakerConfig) {
		if threshold >= 0.0 && threshold <= 1.0 {
			cb.SetFailureRateThreshold(threshold)
		}
	}
}

// WithOpenTimeout sets how long the circuit breaker stays in the Open state
// before transitioning to Half-Open state.
// A timeout that is not positive is ignored.
// Default: 30 seconds.
func WithOpenTimeout(timeout time.Duration) Option {
	return func(cb CircuitBreakerConfig) {
		if timeout > 0 {
			cb.SetOpenTimeout(timeout)
		}
	}
}

// WithRequestTimeout sets the maximum duration for individual requests.
// The context passed to the wrapped function is canceled after this timeout,
// and a request that ends in a deadline error is counted as a failure.
// A timeout that is not positive is ignored.
// Default: 5 seconds.
func WithRequestTimeout(timeout time.Duration) Option {
	return func(cb CircuitBreakerConfig) {
		if timeout > 0 {
			cb.SetRequestTimeout(timeout)
		}
	}
}

// WithThresholdRequestInHalfOpen sets the number of probe requests admitted in
// half-open state. The transition decision is made when all of them have
// completed; requests beyond that number are rejected in the meantime.
// A threshold that is not positive is ignored.
// Default: 10 requests.
func WithThresholdRequestInHalfOpen(threshold int64) Option {
	return func(cb CircuitBreakerConfig) {
		if threshold > 0 {
			cb.SetThresholdRequestInHalfOpen(threshold)
		}
	}
}

// WithTargetRateInHalfOpen sets the target success rate (0.0 to 1.0) required
// to close the circuit when in half-open state.
// Example: 0.6 means 60% success rate is required.
// Both ends of the range are accepted. At 0.0 the circuit closes after the
// probes whatever their outcome.
// A rate outside the range is ignored.
// Default: 0.6 (60%).
func WithTargetRateInHalfOpen(rate float64) Option {
	return func(cb CircuitBreakerConfig) {
		if rate >= 0.0 && rate <= 1.0 {
			cb.SetTargetRateInHalfOpen(rate)
		}
	}
}

// WithCustomErrorPredicate sets a custom function to determine
// which errors should be considered as failures.
// The function should return true if the error represents a failure.
// It is called with non-nil errors only, and not for timeouts, which always
// count as failures. A nil predicate is ignored.
// Default: all non-nil errors are considered failures.
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
// whenever the circuit breaker changes state.
// The callback receives the old state and new state as parameters. It runs in
// a goroutine of its own for every transition.
//
// The parameter is typed any because this package cannot name the State type.
// listener must be exactly a func(from, to circuitbreaker.State). A value of any
// other type is ignored without an error, and so is nil: neither replaces a
// listener set earlier.
//
// Example usage:
//
//	WithStateChangeListener(func(from, to circuitbreaker.State) {
//	    log.Printf("Circuit breaker state changed from %s to %s", from, to)
//	})
func WithStateChangeListener(listener any) Option {
	return func(cb CircuitBreakerConfig) {
		cb.SetStateChangeListener(listener)
	}
}

// WithRequestCompleteListener sets a callback function that gets called
// after every request the circuit breaker admitted, with its outcome and its
// duration. Requests rejected by an open breaker are not reported.
// The callback runs in a goroutine of its own for every request.
// A nil listener removes a listener set earlier.
//
// Example usage:
//
//	WithRequestCompleteListener(func(success bool, d time.Duration) {
//	    requestDuration.WithLabelValues(strconv.FormatBool(success)).Observe(d.Seconds())
//	})
func WithRequestCompleteListener(listener func(success bool, duration time.Duration)) Option {
	return func(cb CircuitBreakerConfig) {
		cb.SetRequestCompleteListener(listener)
	}
}

// Convenience functions for common configurations

// WithDefaults returns the values a breaker uses when it is given no options
// at all, as an explicit base for customization.
func WithDefaults() []Option {
	return []Option{
		WithTimeWindow(60 * time.Second),
		WithMinimumRequestThreshold(10),
		WithFailureRateThreshold(0.5),
		WithOpenTimeout(30 * time.Second),
		WithRequestTimeout(5 * time.Second),
		WithThresholdRequestInHalfOpen(10),
		WithTargetRateInHalfOpen(0.6),
	}
}

// WithTestDefaults returns options for use in tests: like WithDefaults, but with
// a minimum request threshold of 5 and a request timeout of 3 seconds, so a
// breaker trips after a handful of requests.
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

// WithHTTPDefaults returns options optimized for HTTP requests. It leaves the
// half-open settings at their defaults.
//
// Its error predicate counts every non-nil error as a failure, like the
// default one. Append WithCustomErrorPredicate to keep application-level errors
// such as a 4xx response out of the failure rate.
func WithHTTPDefaults() []Option {
	return []Option{
		WithTimeWindow(1 * time.Minute),
		WithMinimumRequestThreshold(10),
		WithFailureRateThreshold(0.5),
		WithOpenTimeout(30 * time.Second),
		WithRequestTimeout(30 * time.Second), // Longer timeout for HTTP
		WithCustomErrorPredicate(func(err error) bool {
			// Every error is a failure, context cancellations included.
			// Callers that want application-level errors left out append
			// their own predicate.
			return err != nil
		}),
	}
}

// WithDatabaseDefaults returns options optimized for database connections. It
// leaves the half-open settings at their defaults.
func WithDatabaseDefaults() []Option {
	return []Option{
		WithTimeWindow(2 * time.Minute),      // Longer window for DB
		WithMinimumRequestThreshold(15),      // Higher threshold
		WithFailureRateThreshold(0.6),        // More tolerant
		WithOpenTimeout(45 * time.Second),    // DB recovery time
		WithRequestTimeout(15 * time.Second), // DB query timeout
	}
}

// WithMicroserviceDefaults returns options optimized for microservice calls. It
// leaves the half-open settings at their defaults.
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
// Its methods mirror the With functions and append one option each; durations
// are given in whole seconds. Build returns the collected options.
type ConfigBuilder struct {
	options []Option
}

// NewConfigBuilder creates a new configuration builder.
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		options: make([]Option, 0),
	}
}

// TimeWindow adds a time window option, in seconds, to the builder.
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

// OpenTimeout adds an open timeout option, in seconds, to the builder.
func (b *ConfigBuilder) OpenTimeout(seconds int64) *ConfigBuilder {
	b.options = append(b.options, WithOpenTimeout(time.Duration(seconds)*time.Second))

	return b
}

// RequestTimeout adds a request timeout option, in seconds, to the builder.
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

// Build returns the configured options in the order they were added.
func (b *ConfigBuilder) Build() []Option {
	return b.options
}
