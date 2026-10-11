package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	circuitbreaker "github.com/anilonayy/circuitbreaker"
	"github.com/anilonayy/circuitbreaker/examples/internal/quotes"
	"github.com/anilonayy/circuitbreaker/options"
	"github.com/gin-gonic/gin"
)

// The middleware answers 502 for a downstream error, 504 for a downstream that
// is too slow and, once those failures have opened the breaker, 503 without
// calling the downstream at all.
func TestBreakerMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var downstreamCalls atomic.Int64

	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstreamCalls.Add(1)
		quotes.Handler().ServeHTTP(w, r)
	}))
	defer downstream.Close()

	cb := circuitbreaker.NewCircuitBreaker(t.Name(),
		options.WithMinimumRequestThreshold(3),
		options.WithFailureRateThreshold(0.5),
		options.WithRequestTimeout(100*time.Millisecond),
		options.WithOpenTimeout(time.Minute),
	)
	t.Cleanup(func() { circuitbreaker.RemoveCircuitBreaker(t.Name()) })

	handler := newRouter(quotes.Client{BaseURL: downstream.URL}, cb)

	steps := []struct {
		mode       string
		wantStatus int
		wantState  circuitbreaker.State
	}{
		{"ok", http.StatusOK, circuitbreaker.StateClosed},
		{"error", http.StatusBadGateway, circuitbreaker.StateClosed},
		// The second failure out of three requests reaches the 50% threshold.
		{"slow", http.StatusGatewayTimeout, circuitbreaker.StateOpen},
		// The downstream would answer this one, but it is not asked.
		{"ok", http.StatusServiceUnavailable, circuitbreaker.StateOpen},
	}

	for i, step := range steps {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/quote?mode="+step.mode, nil))

		if rec.Code != step.wantStatus {
			t.Errorf("request %d (mode=%s): status %d, want %d; body: %s", i+1, step.mode, rec.Code, step.wantStatus, rec.Body)
		}

		if got := cb.GetState(); got != step.wantState {
			t.Errorf("request %d (mode=%s): breaker is %s, want %s", i+1, step.mode, got, step.wantState)
		}
	}

	if got := downstreamCalls.Load(); got != 3 {
		t.Errorf("downstream was called %d times, want 3: the rejected request must not reach it", got)
	}
}
