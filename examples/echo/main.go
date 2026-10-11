// Command echo shows a circuit breaker as Echo middleware.
//
// It starts a pretend downstream service and a server on -addr whose /quote
// route calls it through the breaker:
//
//	go run ./echo
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
	"log"
	"net/http"
	"time"

	circuitbreaker "github.com/anilonayy/circuitbreaker"
	"github.com/anilonayy/circuitbreaker/examples/internal/quotes"
	"github.com/anilonayy/circuitbreaker/options"
	"github.com/labstack/echo/v5"
)

// Breaker returns middleware that runs the next handler through cb and turns
// the breaker's verdict into an HTTP error.
//
// Echo handlers return their error, so the breaker sees how the request went
// without any extra plumbing.
func Breaker(cb *circuitbreaker.CircuitBreaker) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			err := cb.Execute(c.Request().Context(), func(ctx context.Context) error {
				// ctx carries the breaker's request timeout. Putting it on the
				// request is what makes the downstream call stop in time.
				c.SetRequest(c.Request().WithContext(ctx))

				return next(c)
			})

			switch {
			case err == nil:
				return nil
			case errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen):
				// Rejected without calling next: the downstream is given a rest.
				return echo.NewHTTPError(http.StatusServiceUnavailable, "quotes are unavailable, try again later")
			case errors.Is(err, circuitbreaker.ErrCircuitBreakerTimeout):
				return echo.NewHTTPError(http.StatusGatewayTimeout, "quotes did not answer in time")
			default:
				return echo.NewHTTPError(http.StatusBadGateway, "quotes failed").Wrap(err)
			}
		}
	}
}

// newServer wires the routes. Only /quote talks to the downstream, so only
// /quote goes through the breaker.
func newServer(client quotes.Client, cb *circuitbreaker.CircuitBreaker) *echo.Echo {
	e := echo.New()

	e.GET("/quote", func(c *echo.Context) error {
		quote, err := client.Quote(c.Request().Context(), c.QueryParam("mode"))
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, map[string]string{"quote": quote})
	}, Breaker(cb))

	e.GET("/healthz", func(c *echo.Context) error {
		return c.JSON(http.StatusOK, cb.GetDetailedStatistics())
	})

	return e
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
	log.Fatal(newServer(quotes.Client{BaseURL: downstream}, cb).Start(*addr))
}
