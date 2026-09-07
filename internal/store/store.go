package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"time"
)

type Store struct {
	mu   sync.RWMutex
	jobs map[string]*Job

	cancels map[string]context.CancelFunc
}

func New() *Store {
	return &Store{
		jobs:    make(map[string]*Job),
		cancels: make(map[string]context.CancelFunc),
	}
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Store) Create(typ string, payload json.RawMessage) *Job {
	job := &Job{
		ID:        newID(),
		Type:      typ,
		Payload:   payload,
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.ID] = job
	return job.Clone()
}

func (s *Store) Get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	return job.Clone(), true
}

func (s *Store) List(filter Status) []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		if filter != "" && job.Status != filter {
			continue
		}
		out = append(out, job.Clone())
	}

	slices.SortFunc(out, func(a, b *Job) int {
		return b.CreatedAt.Compare(a.CreatedAt)
	})

	return out
}

func (s *Store) Start(id string, cancel context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok || job.Status != StatusQueued {
		return false
	}

	now := time.Now().UTC()
	job.Status = StatusRunning
	job.StartedAt = &now
	s.cancels[id] = cancel
	return true
}

func (s *Store) Finish(id string, result json.RawMessage, err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return false
	}

	delete(s.cancels, id)

	now := time.Now().UTC()
	job.FinishedAt = &now

	switch {
	case err == nil:
		job.Status = StatusSucceeded
		job.Result = result
	case errors.Is(err, context.Canceled):
		job.Status = StatusCanceled
		job.Error = "canceled"
	default:
		job.Status = StatusFailed
		job.Error = err.Error()
	}
	return true
}

func (s *Store) Cancel(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok || job.Status.IsTerminal() {
		return false
	}

	if job.Status == StatusQueued {
		now := time.Now().UTC()
		job.Status = StatusCanceled
		job.FinishedAt = &now
		return true
	}
	if cancel, ok := s.cancels[id]; ok {
		cancel()
	}
	return true
}
