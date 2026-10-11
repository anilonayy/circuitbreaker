# 🚦 Go Circuit Breaker

[![Go Reference](https://pkg.go.dev/badge/github.com/anilonayy/circuitbreaker.svg)](https://pkg.go.dev/github.com/anilonayy/circuitbreaker)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Production‑ready, lightweight Circuit Breaker for Go. Protects your external calls (HTTP, DB, RPC) by monitoring failures in a sliding time window, opening on sustained errors, and probing recovery in half‑open before safely closing.

• Simple API • Sensible defaults • Powerful options • Observability built‑in

---

## 🔧 Install

```
go get github.com/anilonayy/circuitbreaker@latest
```

## ⚡ TL;DR (Quick Start)

```go
package main

import (
    "context"
    "fmt"
    "time"

    circuitbreaker "github.com/anilonayy/circuitbreaker"
    "github.com/anilonayy/circuitbreaker/options"
)

func main() {
    // Start with a preset, then customize if needed
    opts := options.WithHTTPDefaults()
    opts = append(opts,
        options.WithFailureRateThreshold(0.5),      // open if 50%+ failures
        options.WithOpenTimeout(30*time.Second),    // stay open 30s
        options.WithRequestTimeout(5*time.Second),  // per-call timeout
    )
    cb := circuitbreaker.NewCircuitBreaker("payment-service", opts...)

    ctx := context.Background()
    err := cb.Execute(ctx, func(ctx context.Context) error {
        // Do your external call (respect ctx)
        // return an error to count as failure
        return nil
    })

    switch err {
    case nil:
        fmt.Println("ok")
    case circuitbreaker.ErrCircuitBreakerOpen:
        fmt.Println("fast-fail: circuit open")
    case circuitbreaker.ErrCircuitBreakerTimeout:
        fmt.Println("breaker: request timeout")
    default:
        fmt.Printf("call error: %v\n", err)
    }
}
```

## 🚨 Todos

- [x] MVP
- [ ] Add different framework examples (echo, gin etc..)
- [x] Unit tests (`circuitbreaker_test.go`)
- [ ] Add benchmarks
- [ ] Add documentation
- [ ] Add regression tests


---

## 🧭 How It Works

```mermaid
stateDiagram-v2
    [*] --> CLOSED
    CLOSED --> OPEN: failure rate ≥ threshold
    OPEN --> HALF_OPEN: after openTimeout
    HALF_OPEN --> CLOSED: probes success rate ≥ target
    HALF_OPEN --> OPEN: probes success rate < target
```

- Closed: traffic flows; failures are tracked in a sliding time window.
- Open: fast‑fail; rejects immediately; waits `openTimeout` before probing.
- Half‑Open: lets through at most `ThresholdRequestInHalfOpen` probe requests per round, counting the ones still in flight, and rejects the rest with `ErrCircuitBreakerOpen`. Once all probes have completed it closes if their success rate ≥ target, otherwise re‑opens. A probe that times out counts as a failed probe.

---

## ✨ Features

- Sliding window metrics with per‑second buckets
- Configurable thresholds and timeouts
- Half‑open probing with target success ratio
- Custom error predicate (choose what counts as a failure)
- Event hooks: state changes and request completion
- Global registry to manage breakers by key
- Detailed statistics for dashboards/logs

---

## 🧩 Defaults

Values used by `NewCircuitBreaker` when no options are passed. Note that the presets differ (for example `WithDefaults()` sets the minimum request threshold to 100); see `options/options.go`.

| Setting | Default |
| --- | --- |
| Time window | 1 minute |
| Minimum requests to evaluate | 10 |
| Failure rate threshold | 0.5 (50%) |
| Open timeout | 30 seconds |
| Per‑request timeout | 5 seconds |
| Half‑Open probe count | 10 |
| Half‑Open target success | 0.6 (60%) |

---

## ⚙️ Configuration

Import `github.com/anilonayy/circuitbreaker/options` and compose any of:

- `WithTimeWindow(d time.Duration)`
- `WithMinimumRequestThreshold(n int64)`
- `WithFailureRateThreshold(p float64)` (0–1)
- `WithOpenTimeout(d time.Duration)`
- `WithRequestTimeout(d time.Duration)`
- `WithThresholdRequestInHalfOpen(n int64)`
- `WithTargetRateInHalfOpen(p float64)` (0–1)
- `WithCustomErrorPredicate(func(error) bool)`
- `WithStateChangeListener(any)` → `func(from, to State)`
- `WithRequestCompleteListener(func(success bool, d time.Duration))`

Presets and helpers:

- `WithDefaults()`
- `WithHTTPDefaults()` / `WithDatabaseDefaults()` / `WithMicroserviceDefaults()`
- `WithAggressive()` and `WithConservative()`
- Builder pattern:

```go
opts := options.NewConfigBuilder().
    TimeWindow(60).                     // seconds
    MinimumRequestThreshold(20).
    FailureRateThreshold(0.4).
    OpenTimeout(45).
    RequestTimeout(10).
    ThresholdRequestInHalfOpen(8).
    TargetRateInHalfOpen(0.7).
    Build()

cb := circuitbreaker.NewCircuitBreaker("svc", opts...)
```

---

## 🧪 Practical Examples

### HTTP call with custom failure predicate

```go
opts := append(options.WithHTTPDefaults(),
    options.WithCustomErrorPredicate(func(err error) bool {
        // Only treat real transport/timeouts as failures; app 4xx may not be
        return err != nil
    }),
)
cb := circuitbreaker.NewCircuitBreaker("http-upstream", opts...)

err := cb.Execute(context.Background(), func(ctx context.Context) error {
    // net/http client call that respects ctx
    return nil
})
```

### Event hooks for observability

```go
cb := circuitbreaker.NewCircuitBreaker(
    "orders",
    options.WithStateChangeListener(func(from, to circuitbreaker.State) {
        fmt.Printf("state: %s -> %s\n", from, to)
    }),
    options.WithRequestCompleteListener(func(success bool, d time.Duration) {
        fmt.Printf("done: success=%v duration=%s\n", success, d)
    }),
)
```

### Reading metrics

```go
total, ok, fail, rate := cb.GetStatistics()
fmt.Printf("total=%d ok=%d fail=%d rate=%.2f\n", total, ok, fail, rate)

details := cb.GetDetailedStatistics()
fmt.Println(details.State, details.Key)
// includes Open/Half‑Open timestamps, counters, and thresholds
```

### Registry utilities

```go
_ = circuitbreaker.NewCircuitBreaker("billing")
if got, ok := circuitbreaker.GetCircuitBreaker("billing"); ok { _ = got }
for _, k := range circuitbreaker.ListCircuitBreakers() { _ = k }
circuitbreaker.RemoveCircuitBreaker("billing")
```

---

## 🚨 Errors

- `ErrCircuitBreakerOpen`: request rejected without being executed (fast‑fail), because the circuit is Open or because it is Half‑Open and all probe slots are taken
- `ErrCircuitBreakerTimeout`: request exceeded breaker’s per‑request timeout

---

## Notes

- Thread‑safe: a single internal lock protects state, configuration and metrics; admitting a request and recording its result are each atomic, so concurrent callers see one consistent state machine. The suite passes under `go test -race`.
- Event hooks run asynchronously, each call in its own goroutine and outside the lock: they may call back into the breaker, and calls for successive events are not ordered.
- Context‑aware: honors provided `context.Context` and enforces its own timeout.
- `NewCircuitBreaker(key, ...)` registers the breaker under `key`; calling it again with an existing key returns the already-registered breaker and ignores the new options.
- `ExecuteWithTimeout` is an alternative to `Execute` that coordinates goroutine completion around the breaker's timeout.
- Stringer: `cb.String()` prints a compact summary for logs.

---

## License

MIT — see `LICENSE`.

