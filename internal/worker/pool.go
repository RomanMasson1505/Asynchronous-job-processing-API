package worker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/RomanMasson1505/jobapi/internal/store"
)

type Pool struct {
	store    *store.Store
	registry Registry

	queue chan string

	wg      sync.WaitGroup
	workers int
}

func New(st *store.Store, workers, size int) *Pool {
	if workers < 1 {
		workers = 1
	}
	if size < 1 {
		size = 1
	}
	return &Pool{
		store:    st,
		registry: DefaultRegistry(),
		queue:    make(chan string, size),
		workers:  workers,
	}
}

func (p *Pool) Queue() chan<- string {
	return p.queue
}

func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.workers; i++ {
		p.wg.Add(1)
		go p.run(ctx, i)
	}
	log.Printf("worker pool démarré (%d workers, file de %d)", p.workers, cap(p.queue))
}

func (p *Pool) Stop() {
	close(p.queue)
	p.wg.Wait()
	log.Print("worker pool arrêté")
}

func (p *Pool) run(ctx context.Context, id int) {
	defer p.wg.Done()

	for jobID := range p.queue {
		p.process(ctx, jobID)
	}
	log.Printf("worker %d terminé", id)
}

func (p *Pool) process(ctx context.Context, jobID string) {
	job, ok := p.store.Get(jobID)
	if !ok {
		log.Printf("job %s introuvable, ignoré", jobID)
		return
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if !p.store.Start(jobID, cancel) {
		return
	}

	handler, ok := p.registry[job.Type]
	if !ok {
		p.store.Finish(jobID, nil, fmt.Errorf("type de job inconnu: %q", job.Type))
		return
	}

	result, err := handler(jobCtx, job.Payload)
	p.store.Finish(jobID, result, err)
}
