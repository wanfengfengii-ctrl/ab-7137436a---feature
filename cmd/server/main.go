// Command interlock-events serves the accelerator interlock event API:
// idempotent per-channel batch ingestion, resumable exactly-once SSE
// streams, bounded retention and WAL-backed durability.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"interlock-events/internal/api"
	"interlock-events/internal/store"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("invalid %s=%q, using default %d", key, v, def)
	}
	return def
}

func main() {
	port := env("APP_PORT", "8080")
	dataDir := env("DATA_DIR", "/data")
	retention := envInt("RETENTION_LIMIT", 100)
	shutdownTimeout := time.Duration(envInt("SHUTDOWN_TIMEOUT_SECONDS", 10)) * time.Second

	st, err := store.Open(dataDir, retention)
	if err != nil {
		log.Fatalf("open store at %s: %v", dataDir, err)
	}
	log.Printf("store opened: dir=%s retention=%d nextID=%d", dataDir, retention, st.NextID())

	handler := api.New(st, api.Config{Retention: retention})
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// SSE connections are long-lived; do not impose overall read/write
		// deadlines that would kill idle streams between heartbeats.
		IdleTimeout: 120 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("interlock-events listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-stop
	log.Printf("shutdown signal received, draining (up to %s)...", shutdownTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
		_ = srv.Close()
	}
	log.Printf("stopped")
}
