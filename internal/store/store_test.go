package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)


func TestCreateAndGet(t *testing.T) {
	s := New()
	job := s.Create("sleep", json.RawMessage(`{"ms":10}`))

	if job.ID == "" {
		t.Fatal("Create doit générer un ID")
	}
	if job.Status != StatusQueued {
		t.Errorf("statut = %q, attendu %q", job.Status, StatusQueued)
	}
	if job.CreatedAt.IsZero() {
		t.Error("CreatedAt doit être renseigné")
	}
	got, ok := s.Get(job.ID)
	if !ok {
		t.Fatal("le job créé doit être retrouvable")
	}
	if got.ID != job.ID {
		t.Errorf("ID = %q, attendu %q", got.ID, job.ID)
	}

	if _, ok := s.Get("id-qui-nexiste-pas"); ok {
		t.Error("Get doit renvoyer false pour un ID inconnu")
	}
}

func TestGetReturnsCopy(t *testing.T) {
	s := New()
	job := s.Create("sleep", nil)

	first, _ := s.Get(job.ID)
	first.Status = StatusFailed 
	first.Type = "vandalisé"

	second, _ := s.Get(job.ID)
	if second.Status != StatusQueued {
		t.Errorf("modifier la copie a modifié le store : statut = %q", second.Status)
	}
	if second.Type != "sleep" {
		t.Errorf("modifier la copie a modifié le store : type = %q", second.Type)
	}
}

func TestLifecycle(t *testing.T) {
	tests := []struct {
		name       string
		result     json.RawMessage
		err        error
		wantStatus Status
		wantErrMsg string
	}{
		{
			name:       "succès",
			result:     json.RawMessage(`{"ok":true}`),
			err:        nil,
			wantStatus: StatusSucceeded,
		},
		{
			name:       "échec",
			err:        errors.New("boum"),
			wantStatus: StatusFailed,
			wantErrMsg: "boum",
		},
		{
			name:       "annulation",
			err:        context.Canceled,
			wantStatus: StatusCanceled,
			wantErrMsg: "canceled",
		},
		{
			name:       "annulation enveloppée",
			err:        errors.Join(errors.New("abandon"), context.Canceled),
			wantStatus: StatusCanceled,
			wantErrMsg: "canceled",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			job := s.Create("sleep", nil)

			if !s.Start(job.ID, func() {}) {
				t.Fatal("Start doit réussir sur un job en attente")
			}
			running, _ := s.Get(job.ID)
			if running.Status != StatusRunning {
				t.Fatalf("statut = %q, attendu %q", running.Status, StatusRunning)
			}
			if running.StartedAt == nil {
				t.Error("StartedAt doit être renseigné au démarrage")
			}

			if !s.Finish(job.ID, tc.result, tc.err) {
				t.Fatal("Finish doit réussir sur un job existant")
			}
			done, _ := s.Get(job.ID)
			if done.Status != tc.wantStatus {
				t.Errorf("statut = %q, attendu %q", done.Status, tc.wantStatus)
			}
			if done.Error != tc.wantErrMsg {
				t.Errorf("erreur = %q, attendu %q", done.Error, tc.wantErrMsg)
			}
			if done.FinishedAt == nil {
				t.Error("FinishedAt doit être renseigné à la fin")
			}
		})
	}
}

func TestStartRefuseUnJobNonEnAttente(t *testing.T) {
	s := New()
	job := s.Create("sleep", nil)

	s.Start(job.ID, func() {})
	if s.Start(job.ID, func() {}) {
		t.Error("Start doit refuser un job déjà en cours")
	}
	if s.Start("inconnu", func() {}) {
		t.Error("Start doit refuser un job inexistant")
	}
}

func TestCancel(t *testing.T) {
	t.Run("job en attente", func(t *testing.T) {
		s := New()
		job := s.Create("sleep", nil)

		if !s.Cancel(job.ID) {
			t.Fatal("annuler un job en attente doit réussir")
		}
		got, _ := s.Get(job.ID)
		if got.Status != StatusCanceled {
			t.Errorf("statut = %q, attendu %q", got.Status, StatusCanceled)
		}
		if s.Start(job.ID, func() {}) {
			t.Error("un job annulé ne doit plus pouvoir démarrer")
		}
	})

	t.Run("job en cours", func(t *testing.T) {
		s := New()
		job := s.Create("sleep", nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s.Start(job.ID, cancel)

		if !s.Cancel(job.ID) {
			t.Fatal("annuler un job en cours doit réussir")
		}
		select {
		case <-ctx.Done():
		default:
			t.Error("le context du job aurait dû être annulé")
		}
	})

	t.Run("job terminé", func(t *testing.T) {
		s := New()
		job := s.Create("sleep", nil)
		s.Start(job.ID, func() {})
		s.Finish(job.ID, nil, nil)

		if s.Cancel(job.ID) {
			t.Error("on ne doit pas pouvoir annuler un job terminé")
		}
		if s.Cancel("inconnu") {
			t.Error("on ne doit pas pouvoir annuler un job inexistant")
		}
	})
}

func TestList(t *testing.T) {
	s := New()
	a := s.Create("sleep", nil)
	b := s.Create("uppercase", nil)
	s.Start(b.ID, func() {})

	tests := []struct {
		name   string
		filter Status
		want   int
	}{
		{"sans filtre", "", 2},
		{"en attente", StatusQueued, 1},
		{"en cours", StatusRunning, 1},
		{"terminés", StatusSucceeded, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := len(s.List(tc.filter)); got != tc.want {
				t.Errorf("List(%q) a rendu %d jobs, attendu %d", tc.filter, got, tc.want)
			}
		})
	}

	all := s.List("")
	if all[0].ID != b.ID || all[1].ID != a.ID {
		t.Error("List doit trier du plus récent au plus ancien")
	}
}

func TestAccesConcurrent(t *testing.T) {
	s := New()

	const n = 50
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job := s.Create("sleep", json.RawMessage(`{"ms":1}`))
			s.Start(job.ID, func() {})
			s.Get(job.ID)
			s.List("")
			s.Finish(job.ID, json.RawMessage(`{}`), nil)
		}()
	}
	wg.Wait()

	if got := len(s.List("")); got != n {
		t.Errorf("%d jobs enregistrés, attendu %d", got, n)
	}
}
