// Package quotes is the downstream the examples put a circuit breaker in front
// of: a small HTTP service that can be told to misbehave, and a client for it.
package quotes

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Handler returns the downstream service. GET /quote answers with a quote.
// The mode query parameter makes it misbehave on request:
//
//	mode=error  answers 500 at once
//	mode=slow   does not answer until the caller has given up
func Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /quote", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("mode") {
		case "error":
			http.Error(w, "quote database is down", http.StatusInternalServerError)

			return
		case "slow":
			select {
			case <-r.Context().Done():
				return
			case <-time.After(30 * time.Second):
			}
		}

		fmt.Fprint(w, "The best way out is always through.")
	})

	return mux
}

// StartLocal serves Handler on a free local port and returns its base URL. The
// examples use it so that they run without anything else set up.
func StartLocal() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("quotes: %w", err)
	}

	go func() { _ = http.Serve(ln, Handler()) }()

	return "http://" + ln.Addr().String(), nil
}

// Client calls the downstream service.
type Client struct {
	// HTTP is the client to use; http.DefaultClient if nil.
	HTTP *http.Client
	// BaseURL is the address of the service, without a trailing slash.
	BaseURL string
}

// Quote fetches a quote. mode is passed on to the service as it is.
//
// It gives up when ctx is done. The error then wraps ctx's error, which is how
// the circuit breaker in front of this call recognizes a timeout.
func (c Client) Quote(ctx context.Context, mode string) (string, error) {
	target := c.BaseURL + "/quote?mode=" + url.QueryEscape(mode)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", fmt.Errorf("quotes: %w", err)
	}

	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("quotes: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("quotes: downstream answered %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return "", fmt.Errorf("quotes: %w", err)
	}

	return strings.TrimSpace(string(body)), nil
}
