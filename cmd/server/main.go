package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RomanMasson1505/jobapi/internal/api"
	"github.com/RomanMasson1505/jobapi/internal/store"
	"github.com/RomanMasson1505/jobapi/internal/worker"
)

const (
	addr            = ":8080"
	workers         = 4
	queueSize       = 128
	shutdownTimeout = 15 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := store.New()
	pool := worker.New(st, workers, queueSize)
	pool.Start(context.Background())

	srv := &http.Server{
		Addr:    addr,
		Handler: api.NewServer(st, pool.Queue()).Routes(),

		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("écoute sur %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serveur HTTP: %v", err)
		}
	}()

	<-ctx.Done()
	log.Print("signal d'arrêt reçu")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("arrêt HTTP forcé: %v", err)
	}

	pool.Stop()

	log.Print("arrêt propre terminé")
}
