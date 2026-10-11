package options_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	circuitbreaker "github.com/anilonayy/circuitbreaker"
	"github.com/anilonayy/circuitbreaker/options"
)

// The first half of this file checks the options against a recorder: which
// setter an option calls, with which value, and when it calls none. The second
// half applies them to a real breaker and checks what they do to its behavior.

// call is one setter invocation seen by a recorder.
type call struct {
	value  any
	setter string
}

// recorder is a CircuitBreakerConfig that only remembers how it was called.
type recorder struct {
	calls []call
}

func (r *recorder) add(setter string, value any) {
	r.calls = append(r.calls, call{setter: setter, value: value})
}

func (r *recorder) SetTimeWindow(d time.Duration)              { r.add("SetTimeWindow", d) }
func (r *recorder) SetMinimumRequestThreshold(n int64)         { r.add("SetMinimumRequestThreshold", n) }
func (r *recorder) SetFailureRateThreshold(p float64)          { r.add("SetFailureRateThreshold", p) }
func (r *recorder) SetOpenTimeout(d time.Duration)             { r.add("SetOpenTimeout", d) }
func (r *recorder) SetRequestTimeout(d time.Duration)          { r.add("SetRequestTimeout", d) }
func (r *recorder) SetThresholdRequestInHalfOpen(n int64)      { r.add("SetThresholdRequestInHalfOpen", n) }
func (r *recorder) SetTargetRateInHalfOpen(p float64)          { r.add("SetTargetRateInHalfOpen", p) }
func (r *recorder) SetCustomErrorPredicate(p func(error) bool) { r.add("SetCustomErrorPredicate", p) }
func (r *recorder) SetStateChangeListener(l any)               { r.add("SetStateChangeListener", l) }

func (r *recorder) SetRequestCompleteListener(l func(success bool, duration time.Duration)) {
	r.add("SetRequestCompleteListener", l)
}

// apply runs opts against a fresh recorder and returns what it saw.
func apply(opts ...options.Option) []call {
	var r recorder

	for _, opt := range opts {
		opt(&r)
	}

	return r.calls
}

// An option with a usable value calls exactly its own setter with that value.
// An option with a value outside its range calls nothing at all: the breaker
// keeps whatever it had, and no error is reported.
func TestScalarOptions(t *testing.T) {
	none := []call(nil)

	tests := []struct {
		name string
		opt  options.Option
		want []call
	}{
		{"TimeWindow", options.WithTimeWindow(90 * time.Second), []call{{90 * time.Second, "SetTimeWindow"}}},
		{"TimeWindow/sub-second is accepted", options.WithTimeWindow(time.Nanosecond), []call{{time.Nanosecond, "SetTimeWindow"}}},
		{"TimeWindow/zero is ignored", options.WithTimeWindow(0), none},
		{"TimeWindow/negative is ignored", options.WithTimeWindow(-time.Second), none},

		{"MinimumRequestThreshold", options.WithMinimumRequestThreshold(1), []call{{int64(1), "SetMinimumRequestThreshold"}}},
		{"MinimumRequestThreshold/zero is ignored", options.WithMinimumRequestThreshold(0), none},
		{"MinimumRequestThreshold/negative is ignored", options.WithMinimumRequestThreshold(-5), none},

		{"FailureRateThreshold", options.WithFailureRateThreshold(0.25), []call{{0.25, "SetFailureRateThreshold"}}},
		{"FailureRateThreshold/zero is accepted", options.WithFailureRateThreshold(0), []call{{0.0, "SetFailureRateThreshold"}}},
		{"FailureRateThreshold/one is accepted", options.WithFailureRateThreshold(1), []call{{1.0, "SetFailureRateThreshold"}}},
		{"FailureRateThreshold/below zero is ignored", options.WithFailureRateThreshold(-0.01), none},
		{"FailureRateThreshold/above one is ignored", options.WithFailureRateThreshold(1.01), none},
		{"FailureRateThreshold/NaN is ignored", options.WithFailureRateThreshold(math.NaN()), none},
		{"FailureRateThreshold/Inf is ignored", options.WithFailureRateThreshold(math.Inf(1)), none},

		{"OpenTimeout", options.WithOpenTimeout(45 * time.Second), []call{{45 * time.Second, "SetOpenTimeout"}}},
		{"OpenTimeout/zero is ignored", options.WithOpenTimeout(0), none},
		{"OpenTimeout/negative is ignored", options.WithOpenTimeout(-time.Minute), none},

		{"RequestTimeout", options.WithRequestTimeout(250 * time.Millisecond), []call{{250 * time.Millisecond, "SetRequestTimeout"}}},
		{"RequestTimeout/zero is ignored", options.WithRequestTimeout(0), none},
		{"RequestTimeout/negative is ignored", options.WithRequestTimeout(-time.Second), none},

		{"ThresholdRequestInHalfOpen", options.WithThresholdRequestInHalfOpen(3), []call{{int64(3), "SetThresholdRequestInHalfOpen"}}},
		{"ThresholdRequestInHalfOpen/zero is ignored", options.WithThresholdRequestInHalfOpen(0), none},
		{"ThresholdRequestInHalfOpen/negative is ignored", options.WithThresholdRequestInHalfOpen(-1), none},

		{"TargetRateInHalfOpen", options.WithTargetRateInHalfOpen(0.75), []call{{0.75, "SetTargetRateInHalfOpen"}}},
		{"TargetRateInHalfOpen/zero is accepted", options.WithTargetRateInHalfOpen(0), []call{{0.0, "SetTargetRateInHalfOpen"}}},
		{"TargetRateInHalfOpen/one is accepted", options.WithTargetRateInHalfOpen(1), []call{{1.0, "SetTargetRateInHalfOpen"}}},
		{"TargetRateInHalfOpen/below zero is ignored", options.WithTargetRateInHalfOpen(-0.5), none},
		{"TargetRateInHalfOpen/above one is ignored", options.WithTargetRateInHalfOpen(1.5), none},
		{"TargetRateInHalfOpen/NaN is ignored", options.WithTargetRateInHalfOpen(math.NaN()), none},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := apply(tt.opt); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setter calls = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestWithCustomErrorPredicate(t *testing.T) {
	errWanted := errors.New("wanted")

	calls := apply(options.WithCustomErrorPredicate(func(err error) bool { return errors.Is(err, errWanted) }))
	if len(calls) != 1 || calls[0].setter != "SetCustomErrorPredicate" {
		t.Fatalf("setter calls = %+v, want one SetCustomErrorPredicate", calls)
	}

	predicate, ok := calls[0].value.(func(error) bool)
	if !ok {
		t.Fatalf("predicate has type %T", calls[0].value)
	}

	if !predicate(errWanted) || predicate(errors.New("other")) {
		t.Error("the setter did not receive the predicate passed to the option")
	}

	// A nil predicate would make the breaker panic on the first error, so the
	// option drops it and the default predicate stays.
	if calls := apply(options.WithCustomErrorPredicate(nil)); calls != nil {
		t.Errorf("nil predicate: setter calls = %+v, want none", calls)
	}
}

// The listener options hand their argument to the setter as it is. Unlike the
// other options they do not look at it: nil goes through as well, and so does a
// state change listener of the wrong type. What happens to those is decided by
// the breaker; see TestStateChangeListenerOnBreaker.
func TestListenerOptionsPassTheirArgumentThrough(t *testing.T) {
	t.Run("StateChangeListener", func(t *testing.T) {
		called := false
		listener := func(_, _ circuitbreaker.State) { called = true }

		calls := apply(options.WithStateChangeListener(listener))
		if len(calls) != 1 || calls[0].setter != "SetStateChangeListener" {
			t.Fatalf("setter calls = %+v, want one SetStateChangeListener", calls)
		}

		got, ok := calls[0].value.(func(_, _ circuitbreaker.State))
		if !ok {
			t.Fatalf("listener has type %T", calls[0].value)
		}

		got(circuitbreaker.StateClosed, circuitbreaker.StateOpen)

		if !called {
			t.Error("the setter did not receive the listener passed to the option")
		}
	})

	t.Run("StateChangeListener/nil and wrong type", func(t *testing.T) {
		want := []call{{nil, "SetStateChangeListener"}, {"not a func", "SetStateChangeListener"}}

		got := apply(options.WithStateChangeListener(nil), options.WithStateChangeListener("not a func"))
		if !reflect.DeepEqual(got, want) {
			t.Errorf("setter calls = %+v, want %+v", got, want)
		}
	})

	t.Run("RequestCompleteListener", func(t *testing.T) {
		var (
			gotSuccess  bool
			gotDuration time.Duration
		)

		calls := apply(options.WithRequestCompleteListener(func(success bool, d time.Duration) {
			gotSuccess, gotDuration = success, d
		}))
		if len(calls) != 1 || calls[0].setter != "SetRequestCompleteListener" {
			t.Fatalf("setter calls = %+v, want one SetRequestCompleteListener", calls)
		}

		calls[0].value.(func(bool, time.Duration))(true, time.Second)

		if !gotSuccess || gotDuration != time.Second {
			t.Error("the setter did not receive the listener passed to the option")
		}
	})

	t.Run("RequestCompleteListener/nil", func(t *testing.T) {
		calls := apply(options.WithRequestCompleteListener(nil))
		if len(calls) != 1 || calls[0].setter != "SetRequestCompleteListener" {
			t.Fatalf("setter calls = %+v, want one SetRequestCompleteListener", calls)
		}

		if l := calls[0].value.(func(bool, time.Duration)); l != nil {
			t.Error("nil listener arrived as non-nil")
		}
	})
}

// The presets are plain lists of the options above. Their values are part of
// the package's contract: changing one changes how every breaker built from the
// preset behaves, so each is spelled out here.
func TestPresets(t *testing.T) {
	const (
		timeWindow  = "SetTimeWindow"
		minRequests = "SetMinimumRequestThreshold"
		failureRate = "SetFailureRateThreshold"
		openTimeout = "SetOpenTimeout"
		reqTimeout  = "SetRequestTimeout"
		probes      = "SetThresholdRequestInHalfOpen"
		targetRate  = "SetTargetRateInHalfOpen"
	)

	tests := []struct {
		name   string
		preset []options.Option
		want   []call
	}{
		{
			// Not the values NewCircuitBreaker uses without options: the
			// minimum request threshold here is 100, there it is 10.
			name:   "WithDefaults",
			preset: options.WithDefaults(),
			want: []call{
				{time.Minute, timeWindow},
				{int64(100), minRequests},
				{0.5, failureRate},
				{30 * time.Second, openTimeout},
				{5 * time.Second, reqTimeout},
				{int64(10), probes},
				{0.6, targetRate},
			},
		},
		{
			name:   "WithTestDefaults",
			preset: options.WithTestDefaults(),
			want: []call{
				{time.Minute, timeWindow},
				{int64(5), minRequests},
				{0.5, failureRate},
				{30 * time.Second, openTimeout},
				{3 * time.Second, reqTimeout},
				{int64(10), probes},
				{0.6, targetRate},
			},
		},
		{
			name:   "WithAggressive",
			preset: options.WithAggressive(),
			want: []call{
				{30 * time.Second, timeWindow},
				{int64(5), minRequests},
				{0.3, failureRate},
				{2 * time.Minute, openTimeout},
				{3 * time.Second, reqTimeout},
				{int64(5), probes},
				{0.8, targetRate},
			},
		},
		{
			name:   "WithConservative",
			preset: options.WithConservative(),
			want: []call{
				{2 * time.Minute, timeWindow},
				{int64(20), minRequests},
				{0.7, failureRate},
				{15 * time.Second, openTimeout},
				{10 * time.Second, reqTimeout},
				{int64(15), probes},
				{0.5, targetRate},
			},
		},
		{
			// The three presets below leave the half-open settings alone.
			name:   "WithDatabaseDefaults",
			preset: options.WithDatabaseDefaults(),
			want: []call{
				{2 * time.Minute, timeWindow},
				{int64(15), minRequests},
				{0.6, failureRate},
				{45 * time.Second, openTimeout},
				{15 * time.Second, reqTimeout},
			},
		},
		{
			name:   "WithMicroserviceDefaults",
			preset: options.WithMicroserviceDefaults(),
			want: []call{
				{time.Minute, timeWindow},
				{int64(8), minRequests},
				{0.4, failureRate},
				{20 * time.Second, openTimeout},
				{8 * time.Second, reqTimeout},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := apply(tt.preset...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setter calls =\n%+v\nwant\n%+v", got, tt.want)
			}
		})
	}

	t.Run("WithHTTPDefaults", func(t *testing.T) {
		got := apply(options.WithHTTPDefaults()...)

		want := []call{
			{time.Minute, timeWindow},
			{int64(10), minRequests},
			{0.5, failureRate},
			{30 * time.Second, openTimeout},
			{30 * time.Second, reqTimeout},
		}
		if len(got) != len(want)+1 || !reflect.DeepEqual(got[:len(want)], want) {
			t.Fatalf("setter calls =\n%+v\nwant\n%+v plus an error predicate", got, want)
		}

		last := got[len(want)]
		if last.setter != "SetCustomErrorPredicate" {
			t.Fatalf("last setter = %s, want SetCustomErrorPredicate", last.setter)
		}

		// The preset's predicate treats every non-nil error as a failure, which
		// is what the breaker does by default anyway.
		predicate := last.value.(func(error) bool)
		for _, err := range []error{errors.New("boom"), context.Canceled, context.DeadlineExceeded} {
			if !predicate(err) {
				t.Errorf("predicate(%v) = false, want true", err)
			}
		}

		if predicate(nil) {
			t.Error("predicate(nil) = true, want false")
		}
	})

	t.Run("presets return independent slices", func(t *testing.T) {
		// README tells callers to append to a preset; that must not leak into
		// the next caller's preset.
		first := options.WithHTTPDefaults()
		_ = append(first, options.WithOpenTimeout(time.Hour))

		if got := len(options.WithHTTPDefaults()); got != len(first) {
			t.Errorf("preset has %d options after an append to an earlier copy, want %d", got, len(first))
		}
	})
}

func TestConfigBuilder(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		opts := options.NewConfigBuilder().Build()
		if opts == nil || len(opts) != 0 {
			t.Errorf("Build() on a new builder = %v, want an empty non-nil slice", opts)
		}
	})

	t.Run("options are built in call order, durations given in seconds", func(t *testing.T) {
		opts := options.NewConfigBuilder().
			TimeWindow(60).
			MinimumRequestThreshold(20).
			FailureRateThreshold(0.4).
			OpenTimeout(45).
			RequestTimeout(10).
			ThresholdRequestInHalfOpen(8).
			TargetRateInHalfOpen(0.7).
			Build()

		want := []call{
			{60 * time.Second, "SetTimeWindow"},
			{int64(20), "SetMinimumRequestThreshold"},
			{0.4, "SetFailureRateThreshold"},
			{45 * time.Second, "SetOpenTimeout"},
			{10 * time.Second, "SetRequestTimeout"},
			{int64(8), "SetThresholdRequestInHalfOpen"},
			{0.7, "SetTargetRateInHalfOpen"},
		}
		if got := apply(opts...); !reflect.DeepEqual(got, want) {
			t.Errorf("setter calls =\n%+v\nwant\n%+v", got, want)
		}
	})

	t.Run("callbacks", func(t *testing.T) {
		opts := options.NewConfigBuilder().
			CustomErrorPredicate(func(error) bool { return true }).
			StateChangeListener(func(_, _ circuitbreaker.State) {}).
			RequestCompleteListener(func(bool, time.Duration) {}).
			Build()

		setters := make([]string, 0, len(opts))
		for _, c := range apply(opts...) {
			setters = append(setters, c.setter)
		}

		want := []string{"SetCustomErrorPredicate", "SetStateChangeListener", "SetRequestCompleteListener"}
		if !slices.Equal(setters, want) {
			t.Errorf("setters = %v, want %v", setters, want)
		}
	})

	t.Run("out-of-range values are dropped like in the plain options", func(t *testing.T) {
		opts := options.NewConfigBuilder().
			TimeWindow(0).
			MinimumRequestThreshold(0).
			FailureRateThreshold(2).
			OpenTimeout(-1).
			RequestTimeout(0).
			ThresholdRequestInHalfOpen(-3).
			TargetRateInHalfOpen(-1).
			CustomErrorPredicate(nil).
			Build()

		// The builder still returns one option per call; each of them is a no-op.
		if len(opts) != 8 {
			t.Errorf("Build() returned %d options, want 8", len(opts))
		}

		if calls := apply(opts...); calls != nil {
			t.Errorf("setter calls = %+v, want none", calls)
		}
	})

	t.Run("a later call wins", func(t *testing.T) {
		opts := options.NewConfigBuilder().OpenTimeout(10).OpenTimeout(20).Build()

		want := []call{{10 * time.Second, "SetOpenTimeout"}, {20 * time.Second, "SetOpenTimeout"}}
		if got := apply(opts...); !reflect.DeepEqual(got, want) {
			t.Errorf("setter calls = %+v, want %+v", got, want)
		}
	})
}

// From here on the options are applied to a real breaker.

var (
	errBoom    = errors.New("boom")
	breakerSeq atomic.Int64
)

// newBreaker registers a breaker under a key unique to the calling test and
// removes it when the test ends.
func newBreaker(t *testing.T, opts ...options.Option) *circuitbreaker.CircuitBreaker {
	t.Helper()

	key := fmt.Sprintf("options/%s#%d", t.Name(), breakerSeq.Add(1))
	t.Cleanup(func() { circuitbreaker.RemoveCircuitBreaker(key) })

	return circuitbreaker.NewCircuitBreaker(key, opts...)
}

func succeed(context.Context) error { return nil }

func fail(context.Context) error { return errBoom }

func requireState(t *testing.T, cb *circuitbreaker.CircuitBreaker, want circuitbreaker.State) {
	t.Helper()

	if got := cb.GetState(); got != want {
		t.Fatalf("state = %s, want %s", got, want)
	}
}

// config is the part of DetailedStatistics that mirrors the scalar options.
type config struct {
	TimeWindowSeconds, MinimumRequests int64
	FailureRate                        float64
	OpenTimeoutSeconds, RequestTimeout int64
	HalfOpenProbes                     int64
	HalfOpenTargetRate                 float64
}

func configOf(cb *circuitbreaker.CircuitBreaker) config {
	s := cb.GetDetailedStatistics()

	return config{
		TimeWindowSeconds:  s.TimeWindowSeconds,
		MinimumRequests:    s.MinimumRequestThreshold,
		FailureRate:        s.FailureRateThreshold,
		OpenTimeoutSeconds: s.OpenTimeoutSeconds,
		RequestTimeout:     s.RequestTimeoutSeconds,
		HalfOpenProbes:     s.ThresholdRequestInHalfOpen,
		HalfOpenTargetRate: s.TargetRateInHalfOpen,
	}
}

// breakerDefaults is what NewCircuitBreaker sets when it gets no options.
var breakerDefaults = config{
	TimeWindowSeconds:  60,
	MinimumRequests:    10,
	FailureRate:        0.5,
	OpenTimeoutSeconds: 30,
	RequestTimeout:     5,
	HalfOpenProbes:     10,
	HalfOpenTargetRate: 0.6,
}

func TestOptionsConfigureTheBreaker(t *testing.T) {
	t.Run("no options", func(t *testing.T) {
		if got := configOf(newBreaker(t)); got != breakerDefaults {
			t.Errorf("config = %+v, want the defaults %+v", got, breakerDefaults)
		}
	})

	t.Run("every scalar option", func(t *testing.T) {
		cb := newBreaker(t,
			options.WithTimeWindow(2*time.Minute),
			options.WithMinimumRequestThreshold(7),
			options.WithFailureRateThreshold(0.25),
			options.WithOpenTimeout(12*time.Second),
			options.WithRequestTimeout(3*time.Second),
			options.WithThresholdRequestInHalfOpen(4),
			options.WithTargetRateInHalfOpen(0.9),
		)

		want := config{120, 7, 0.25, 12, 3, 4, 0.9}
		if got := configOf(cb); got != want {
			t.Errorf("config = %+v, want %+v", got, want)
		}
	})

	t.Run("out-of-range values leave the defaults in place", func(t *testing.T) {
		cb := newBreaker(t,
			options.WithTimeWindow(0),
			options.WithMinimumRequestThreshold(-1),
			options.WithFailureRateThreshold(1.5),
			options.WithOpenTimeout(-time.Second),
			options.WithRequestTimeout(0),
			options.WithThresholdRequestInHalfOpen(0),
			options.WithTargetRateInHalfOpen(math.NaN()),
		)

		if got := configOf(cb); got != breakerDefaults {
			t.Errorf("config = %+v, want the defaults %+v", got, breakerDefaults)
		}
	})

	t.Run("an out-of-range value does not undo an earlier valid one", func(t *testing.T) {
		cb := newBreaker(t,
			options.WithOpenTimeout(12*time.Second),
			options.WithOpenTimeout(0),
		)

		if got := configOf(cb).OpenTimeoutSeconds; got != 12 {
			t.Errorf("OpenTimeoutSeconds = %d, want 12", got)
		}
	})

	t.Run("options appended to a preset override it", func(t *testing.T) {
		cb := newBreaker(t, append(options.WithAggressive(), options.WithOpenTimeout(9*time.Second))...)

		want := config{30, 5, 0.3, 9, 3, 5, 0.8}
		if got := configOf(cb); got != want {
			t.Errorf("config = %+v, want %+v", got, want)
		}
	})

	t.Run("a preset without half-open settings keeps the half-open defaults", func(t *testing.T) {
		cb := newBreaker(t, options.WithDatabaseDefaults()...)

		want := config{120, 15, 0.6, 45, 15, breakerDefaults.HalfOpenProbes, breakerDefaults.HalfOpenTargetRate}
		if got := configOf(cb); got != want {
			t.Errorf("config = %+v, want %+v", got, want)
		}
	})
}

// The minimum request threshold and the failure rate threshold decide together
// when a closed breaker opens.
func TestThresholdOptionsDecideWhenTheBreakerOpens(t *testing.T) {
	ctx := context.Background()

	cb := newBreaker(t,
		options.WithMinimumRequestThreshold(4),
		options.WithFailureRateThreshold(0.5),
	)

	// Three failures out of three: the rate is far above the threshold, but
	// the minimum of four requests has not been reached.
	for range 3 {
		_ = cb.Execute(ctx, fail)
	}

	requireState(t, cb, circuitbreaker.StateClosed)

	// The fourth request reaches the minimum. Three of four failed.
	_ = cb.Execute(ctx, succeed)
	requireState(t, cb, circuitbreaker.StateOpen)

	if err := cb.Execute(ctx, succeed); !errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen) {
		t.Errorf("Execute on the open breaker returned %v, want %v", err, circuitbreaker.ErrCircuitBreakerOpen)
	}
}

// The boundaries of the two rate options are accepted, and they are extreme
// settings rather than "off" switches. These tests record what they do today so
// that changing it is a decision and not an accident.
func TestRateBoundaries(t *testing.T) {
	ctx := context.Background()

	t.Run("failure rate threshold 0 opens the breaker without a single failure", func(t *testing.T) {
		cb := newBreaker(t,
			options.WithMinimumRequestThreshold(3),
			options.WithFailureRateThreshold(0),
		)

		for range 3 {
			if err := cb.Execute(ctx, succeed); err != nil {
				t.Fatalf("Execute returned %v", err)
			}
		}

		// 0 failures / 3 requests = 0, and 0 >= 0.
		requireState(t, cb, circuitbreaker.StateOpen)
	})

	t.Run("failure rate threshold 1 opens only when every request failed", func(t *testing.T) {
		cb := newBreaker(t,
			options.WithMinimumRequestThreshold(3),
			options.WithFailureRateThreshold(1),
		)

		_ = cb.Execute(ctx, succeed)

		for range 20 {
			_ = cb.Execute(ctx, fail)
		}

		requireState(t, cb, circuitbreaker.StateClosed)
	})

	t.Run("half-open target rate 0 closes the breaker although every probe failed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cb := newBreaker(t,
				options.WithMinimumRequestThreshold(1),
				options.WithOpenTimeout(time.Second),
				options.WithThresholdRequestInHalfOpen(2),
				options.WithTargetRateInHalfOpen(0),
				// Keeps the breaker from reopening on the failure rate the
				// moment it closes, so the half-open decision stays visible.
				options.WithFailureRateThreshold(1),
			)

			_ = cb.Execute(ctx, fail)
			requireState(t, cb, circuitbreaker.StateOpen)

			time.Sleep(time.Second)

			_ = cb.Execute(ctx, fail)
			requireState(t, cb, circuitbreaker.StateHalfOpen)

			_ = cb.Execute(ctx, fail)

			// 0 successes / 2 probes = 0, and 0 >= 0.
			requireState(t, cb, circuitbreaker.StateClosed)
		})
	})
}

func TestOpenTimeoutAndHalfOpenOptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		cb := newBreaker(t,
			options.WithMinimumRequestThreshold(1),
			options.WithOpenTimeout(10*time.Second),
			options.WithThresholdRequestInHalfOpen(4),
			options.WithTargetRateInHalfOpen(0.75),
		)

		_ = cb.Execute(ctx, fail)
		requireState(t, cb, circuitbreaker.StateOpen)

		// One nanosecond short of the open timeout the breaker still rejects.
		time.Sleep(10*time.Second - time.Nanosecond)

		if err := cb.Execute(ctx, succeed); !errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen) {
			t.Fatalf("Execute before the open timeout returned %v, want %v", err, circuitbreaker.ErrCircuitBreakerOpen)
		}

		time.Sleep(time.Nanosecond)

		// Four probes; three successes are exactly the 75% target.
		for i, fn := range []func(context.Context) error{succeed, fail, succeed} {
			_ = cb.Execute(ctx, fn)

			if got := cb.GetState(); got != circuitbreaker.StateHalfOpen {
				t.Fatalf("state after probe %d = %s, want %s", i+1, got, circuitbreaker.StateHalfOpen)
			}
		}

		_ = cb.Execute(ctx, succeed)
		requireState(t, cb, circuitbreaker.StateClosed)
	})
}

func TestRequestTimeoutOption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := newBreaker(t, options.WithRequestTimeout(200*time.Millisecond))

		start := time.Now()

		err := cb.Execute(context.Background(), func(ctx context.Context) error {
			<-ctx.Done()

			return ctx.Err()
		})
		if !errors.Is(err, circuitbreaker.ErrCircuitBreakerTimeout) {
			t.Errorf("Execute returned %v, want %v", err, circuitbreaker.ErrCircuitBreakerTimeout)
		}

		if got := time.Since(start); got != 200*time.Millisecond {
			t.Errorf("fn's context expired after %v, want 200ms", got)
		}
	})
}

func TestTimeWindowOption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		cb := newBreaker(t,
			options.WithTimeWindow(10*time.Second),
			options.WithMinimumRequestThreshold(4),
		)

		for range 3 {
			_ = cb.Execute(ctx, fail)
		}

		// The three failures have left the window by now, so the fourth one
		// stands alone and the minimum of four is not reached.
		time.Sleep(12 * time.Second)

		_ = cb.Execute(ctx, fail)
		requireState(t, cb, circuitbreaker.StateClosed)

		if total, _, failures, _ := cb.GetStatistics(); total != 1 || failures != 1 {
			t.Errorf("statistics = total %d, failures %d; want 1, 1", total, failures)
		}
	})
}

func TestCustomErrorPredicateOnBreaker(t *testing.T) {
	ctx := context.Background()
	errIgnored := errors.New("not found")

	cb := newBreaker(t,
		options.WithMinimumRequestThreshold(2),
		options.WithCustomErrorPredicate(func(err error) bool { return !errors.Is(err, errIgnored) }),
	)

	for range 5 {
		// The error still reaches the caller; it just does not count.
		err := cb.Execute(ctx, func(context.Context) error { return errIgnored })
		if !errors.Is(err, errIgnored) {
			t.Fatalf("Execute returned %v, want %v", err, errIgnored)
		}
	}

	requireState(t, cb, circuitbreaker.StateClosed)

	if total, successes, failures, _ := cb.GetStatistics(); total != 5 || successes != 5 || failures != 0 {
		t.Errorf("statistics = total %d, success %d, failure %d; want 5, 5, 0", total, successes, failures)
	}

	t.Run("nil keeps the default predicate", func(t *testing.T) {
		cb := newBreaker(t,
			options.WithMinimumRequestThreshold(1),
			options.WithCustomErrorPredicate(nil),
		)

		_ = cb.Execute(ctx, fail)
		requireState(t, cb, circuitbreaker.StateOpen)
	})
}

// transitions collects what a state change listener is told.
type transitions struct {
	mu  sync.Mutex
	got []string
}

func (l *transitions) record(from, to circuitbreaker.State) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.got = append(l.got, from.String()+"->"+to.String())
}

func (l *transitions) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.got)
}

// WithStateChangeListener takes any, because the options package cannot name
// the breaker's State type. The breaker accepts exactly func(from, to State)
// and silently ignores everything else.
func TestStateChangeListenerOnBreaker(t *testing.T) {
	trip := func(t *testing.T, opts ...options.Option) {
		t.Helper()

		cb := newBreaker(t, append([]options.Option{options.WithMinimumRequestThreshold(1)}, opts...)...)

		_ = cb.Execute(context.Background(), fail)
		requireState(t, cb, circuitbreaker.StateOpen)

		synctest.Wait() // the listener, if any, has run
	}

	want := []string{"CLOSED->OPEN"}

	t.Run("func(from, to State) is called", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log transitions

			trip(t, options.WithStateChangeListener(log.record))

			if got := log.snapshot(); !slices.Equal(got, want) {
				t.Errorf("transitions = %v, want %v", got, want)
			}
		})
	})

	t.Run("a value of another type is ignored and keeps the earlier listener", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log transitions

			type namedListener func(from, to circuitbreaker.State)

			trip(t,
				options.WithStateChangeListener(log.record),
				options.WithStateChangeListener(func(from, to string) { t.Error("listener with string parameters was called") }),
				options.WithStateChangeListener(func(to circuitbreaker.State) { t.Error("one-argument listener was called") }),
				// Even a defined type with the right underlying signature.
				options.WithStateChangeListener(namedListener(func(_, _ circuitbreaker.State) {
					t.Error("listener of a defined func type was called")
				})),
				options.WithStateChangeListener("not a func"),
			)

			if got := log.snapshot(); !slices.Equal(got, want) {
				t.Errorf("transitions = %v, want %v", got, want)
			}
		})
	})

	t.Run("nil does not remove the listener", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var log transitions

			trip(t,
				options.WithStateChangeListener(log.record),
				options.WithStateChangeListener(nil),
			)

			if got := log.snapshot(); !slices.Equal(got, want) {
				t.Errorf("transitions = %v, want %v", got, want)
			}
		})
	})
}

func TestRequestCompleteListenerOnBreaker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		var (
			mu  sync.Mutex
			got []bool
		)

		listener := func(success bool, _ time.Duration) {
			mu.Lock()
			defer mu.Unlock()

			got = append(got, success)
		}

		cb := newBreaker(t, options.WithRequestCompleteListener(listener))

		// Listeners run in their own goroutines; waiting after each request
		// keeps the order of the log deterministic.
		_ = cb.Execute(ctx, succeed)
		synctest.Wait()

		_ = cb.Execute(ctx, fail)
		synctest.Wait()

		mu.Lock()
		defer mu.Unlock()

		if want := []bool{true, false}; !slices.Equal(got, want) {
			t.Errorf("listener saw %v, want %v", got, want)
		}
	})

	t.Run("nil removes the listener", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cb := newBreaker(t,
				options.WithRequestCompleteListener(func(bool, time.Duration) {
					t.Error("removed listener was called")
				}),
				options.WithRequestCompleteListener(nil),
			)

			if err := cb.Execute(context.Background(), succeed); err != nil {
				t.Errorf("Execute returned %v", err)
			}

			synctest.Wait()
		})
	})
}
