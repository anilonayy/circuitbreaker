// Command gin shows a circuit breaker as Gin middleware.
//
// It starts a pretend downstream service and a server on -addr whose /quote
// route calls it through the breaker:
//
//	go run ./gin
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
	"github.com/gin-gonic/gin"
)

// Breaker returns middleware that runs the rest of the handler chain through
// cb and turns the breaker's verdict into a response.
//
// Handlers behind it report a failure with c.Error and leave the response to
// the middleware.
func Breaker(cb *circuitbreaker.CircuitBreaker) gin.HandlerFunc {
	return func(c *gin.Context) {
		err := cb.Execute(c.Request.Context(), func(ctx context.Context) error {
			// ctx carries the breaker's request timeout. Putting it on the
			// request is what makes the downstream call stop in time.
			c.Request = c.Request.WithContext(ctx)
			c.Next()

			if last := c.Errors.Last(); last != nil {
				return last.Err
			}

			return nil
		})

		// The Abort calls matter for the 503: on a rejected request c.Next was
		// never called, and without Abort Gin would run the handler anyway.
		switch {
		case err == nil:
			// The handler has written the response.
		case errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen):
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "quotes are unavailable, try again later"})
		case errors.Is(err, circuitbreaker.ErrCircuitBreakerTimeout):
			c.AbortWithStatusJSON(http.StatusGatewayTimeout, gin.H{"error": "quotes did not answer in time"})
		default:
			c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{"error": "quotes failed"})
		}
	}
}

// newRouter wires the routes. Only /quote talks to the downstream, so only
// /quote goes through the breaker.
func newRouter(client quotes.Client, cb *circuitbreaker.CircuitBreaker) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/quote", Breaker(cb), func(c *gin.Context) {
		quote, err := client.Quote(c.Request.Context(), c.Query("mode"))
		if err != nil {
			_ = c.Error(err)

			return
		}

		c.JSON(http.StatusOK, gin.H{"quote": quote})
	})

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, cb.GetDetailedStatistics())
	})

	return r
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
	log.Fatal(newRouter(quotes.Client{BaseURL: downstream}, cb).Run(*addr))
}
