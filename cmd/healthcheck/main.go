// Command healthcheck is the container HEALTHCHECK probe: it GETs the
// service /health endpoint and exits 0 only when it answers 200. Kept as a
// tiny static binary so the runtime image needs neither shell nor curl.
package main

import (
	"net/http"
	"os"
	"time"
)

func main() {
	url := os.Getenv("HEALTH_URL")
	if url == "" {
		url = "http://localhost:8080/health"
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}
