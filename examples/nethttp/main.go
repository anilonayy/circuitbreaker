// Command nethttp shows a circuit breaker as net/http middleware.
//
// It starts a pretend downstream service and a server on -addr whose /quote
// route calls it through the breaker:
//
//	go run ./nethttp
//	curl -i 'localhost:8080/quote'              # 200
//	curl -i 'localhost:8080/quote?mode=error'   # 502, the downstream failed
//	curl -i 'localhost:8080/quote?mode=slow'    # 504, the breaker's request timeout
//
// After enough failures the breaker opens and every request, mode or not, gets
// a 503 without the downstream being called, until the open timeout has passed.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	circuitbreaker "github.com/anilonayy/circuitbreaker"
	"github.com/anilonayy/circuitbreaker/examples/internal/quotes"
	"github.com/anilonayy/circuitbreaker/options"
)

// handlerFunc is an http.HandlerFunc that reports a failure by returning it.
// net/http handlers have no return value, and the breaker needs to know how the
// request went. A handler that returns an error must not have written a
// response; the middleware does that.
type handlerFunc func(w http.ResponseWriter, r *http.Request) error

// withBreaker runs next through cb and turns the breaker's verdict into a
// response.
func withBreaker(cb *circuitbreaker.CircuitBreaker, next handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := cb.Execute(r.Context(), func(ctx context.Context) error {
			// ctx carries the breaker's request timeout. Handing it to the
			// handler is what makes the downstream call stop in time.
			return next(w, r.WithContext(ctx))
		})

		switch {
		case err == nil:
			// next has written the response.
		case errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen):
			// Rejected without calling next: the downstream is given a rest.
			http.Error(w, "quotes are unavailable, try again later", http.StatusServiceUnavailable)
		case errors.Is(err, circuitbreaker.ErrCircuitBreakerTimeout):
			http.Error(w, "quotes did not answer in time", http.StatusGatewayTimeout)
		default:
			http.Error(w, "quotes failed", http.StatusBadGateway)
		}
	})
}

// newHandler wires the routes. Only /quote talks to the downstream, so only
// /quote goes through the breaker.
func newHandler(client quotes.Client, cb *circuitbreaker.CircuitBreaker) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /quote", withBreaker(cb, func(w http.ResponseWriter, r *http.Request) error {
		quote, err := client.Quote(r.Context(), r.URL.Query().Get("mode"))
		if err != nil {
			return err
		}

		_, err = fmt.Fprintln(w, quote)

		return err
	}))

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, cb)
	})

	return mux
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "address to listen on")
	flag.Parse()

	downstream, err := quotes.StartLocal()
	if err != nil {
		log.Fatal(err)
	}

	cb := circuitbreaker.NewCircuitBreaker("quotes",
		options.WithMinimumRequestThreshold(4),    // judge after 4 requests
		options.WithFailureRateThreshold(0.5),     // open at 50% failures
		options.WithRequestTimeout(time.Second),   // what "slow" means
		options.WithOpenTimeout(10*time.Second),   // rest before probing
		options.WithThresholdRequestInHalfOpen(2), // probes per round
		options.WithStateChangeListener(func(from, to circuitbreaker.State) {
			log.Printf("breaker quotes: %s -> %s", from, to)
		}),
	)

	log.Printf("listening on %s, downstream on %s", *addr, downstream)
	log.Fatal(http.ListenAndServe(*addr, newHandler(quotes.Client{BaseURL: downstream}, cb)))
}
