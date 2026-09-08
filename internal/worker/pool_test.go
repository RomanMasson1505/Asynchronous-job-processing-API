package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/RomanMasson1505/jobapi/internal/store"
)


func waitForStatus(t *testing.T, st *store.Store, id string, want store.Status, timeout time.Duration) *store.Job {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, ok := st.Get(id)
		if ok && job.Status == want {
			return job
		}
		time.Sleep(time.Millisecond)
	}

	job, _ := st.Get(id)
	t.Fatalf("le job %s n'a pas atteint %q en %s (statut actuel: %q)", id, want, timeout, job.Status)
	return nil
}

func newTestPool(t *testing.T) (*store.Store, *Pool) {
	t.Helper()

	st := store.New()
	p := New(st, 2, 10)
	p.Start(context.Background())
	t.Cleanup(p.Stop)
	return st, p
}

func TestPoolExecuteLesJobs(t *testing.T) {
	tests := []struct {
		name       string
		jobType    string
		payload    string
		wantStatus store.Status
		wantResult string
	}{
		{
			name:       "uppercase",
			jobType:    "uppercase",
			payload:    `{"text":"bonjour"}`,
			wantStatus: store.StatusSucceeded,
			wantResult: `{"text":"BONJOUR"}`,
		},
		{
			name:       "sleep",
			jobType:    "sleep",
			payload:    `{"ms":5}`,
			wantStatus: store.StatusSucceeded,
			wantResult: `{"slept_ms":5}`,
		},
		{
			name:       "type inconnu",
			jobType:    "encoder-une-video",
			payload:    `{}`,
			wantStatus: store.StatusFailed,
		},
		{
			name:       "payload invalide",
			jobType:    "uppercase",
			payload:    `{"text":123}`,
			wantStatus: store.StatusFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, p := newTestPool(t)

			job := st.Create(tc.jobType, json.RawMessage(tc.payload))
			p.Queue() <- job.ID

			done := waitForStatus(t, st, job.ID, tc.wantStatus, 2*time.Second)

			if tc.wantResult != "" && string(done.Result) != tc.wantResult {
				t.Errorf("résultat = %s, attendu %s", done.Result, tc.wantResult)
			}
			if tc.wantStatus == store.StatusFailed && done.Error == "" {
				t.Error("un job échoué doit porter un message d'erreur")
			}
			if done.StartedAt == nil || done.FinishedAt == nil {
				t.Error("StartedAt et FinishedAt doivent être renseignés")
			}
		})
	}
}


func TestAnnulationPendantExecution(t *testing.T) {
	st, p := newTestPool(t)

	job := st.Create("sleep", json.RawMessage(`{"ms":10000}`))
	p.Queue() <- job.ID

	waitForStatus(t, st, job.ID, store.StatusRunning, time.Second)

	if !st.Cancel(job.ID) {
		t.Fatal("Cancel doit réussir sur un job en cours")
	}

	done := waitForStatus(t, st, job.ID, store.StatusCanceled, time.Second)
	if done.Result != nil {
		t.Error("un job annulé ne doit pas porter de résultat")
	}
}

func TestAnnulationAvantExecution(t *testing.T) {
	st := store.New()
	p := New(st, 1, 10)

	job := st.Create("uppercase", json.RawMessage(`{"text":"salut"}`))
	p.Queue() <- job.ID

	if !st.Cancel(job.ID) {
		t.Fatal("Cancel doit réussir sur un job en attente")
	}

	p.Start(context.Background())
	p.Stop()

	got, _ := st.Get(job.ID)
	if got.Status != store.StatusCanceled {
		t.Errorf("statut = %q, attendu %q", got.Status, store.StatusCanceled)
	}
	if got.Result != nil {
		t.Error("le job ne devait pas être exécuté")
	}
}


func TestStopVideLaFile(t *testing.T) {
	st := store.New()
	p := New(st, 4, 20)
	p.Start(context.Background())

	const n = 20
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		job := st.Create("sleep", json.RawMessage(`{"ms":1}`))
		ids = append(ids, job.ID)
		p.Queue() <- job.ID
	}

	p.Stop()

	for _, id := range ids {
		job, _ := st.Get(id)
		if job.Status != store.StatusSucceeded {
			t.Errorf("job %s: statut = %q, attendu %q", id, job.Status, store.StatusSucceeded)
		}
	}
}
