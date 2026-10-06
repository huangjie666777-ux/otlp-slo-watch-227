package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	cfg := LoadConfig()
	store := NewStore(cfg.MaxSeries, time.Duration(cfg.ExpirySeconds)*time.Second)
	srv := NewServer(store)
	srv.StartExpiryLoop(time.Second, make(chan struct{}))
	addr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("otlp_slo227 listening on %s (max_series=%d expiry=%ds)", addr, cfg.MaxSeries, cfg.ExpirySeconds)
	log.Fatal(http.ListenAndServe(addr, srv.Handler()))
}
