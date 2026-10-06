package main

import (
	"io"
	"net/http"
	"time"
)

type Server struct {
	store *Store
	mux   *http.ServeMux
}

func NewServer(st *Store) *Server {
	s := &Server{store: st, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /metrics", s.handleScrape)
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 32<<20))
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	updates, err := parseRequest(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.store.Apply(updates); err != nil {
		status := http.StatusBadRequest
		if err == errLimit {
			status = http.StatusTooManyRequests
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte{})
}

func (s *Server) handleScrape(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	io.WriteString(w, Render(s.store))
}

func (s *Server) StartExpiryLoop(interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.store.ExpireIdle()
			case <-stop:
				return
			}
		}
	}()
}
