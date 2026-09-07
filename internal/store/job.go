package store

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
)

func (s Status) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCanceled
}

func (s Status) IsValid() bool {
	switch s {
	case StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusCanceled:
		return true
	}
	return false
}

type Job struct {
	ID   string `json:"id"`
	Type string `json:"type"`

	Payload json.RawMessage `json:"payload,omitempty"`

	Status Status `json:"status"`

	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`

	CreatedAt time.Time `json:"created_at"`

	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

func (j *Job) Clone() *Job {
	c := *j
	if j.StartedAt != nil {
		t := *j.StartedAt
		c.StartedAt = &t
	}
	if j.FinishedAt != nil {
		t := *j.FinishedAt
		c.FinishedAt = &t
	}
	return &c
}
