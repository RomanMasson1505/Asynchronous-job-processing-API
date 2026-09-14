package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/RomanMasson1505/jobapi/internal/store"
)

func newTestServer(t *testing.T) (*store.Store, http.Handler, chan string) {
	t.Helper()

	st := store.New()
	queue := make(chan string, 100)
	return st, NewServer(st, queue).Routes(), queue
}

func do(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	_, h, _ := newTestServer(t)

	rec := do(h, http.MethodGet, "/healthz", "")
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d, attendu %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, attendu du JSON", ct)
	}
}

func TestCreateJob(t *testing.T) {
	_, h, queue := newTestServer(t)

	rec := do(h, http.MethodPost, "/jobs", `{"type":"uppercase","payload":{"text":"salut"}}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("code = %d, attendu %d (corps: %s)", rec.Code, http.StatusAccepted, rec.Body)
	}

	var job store.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
		t.Fatalf("réponse illisible: %v", err)
	}
	if job.ID == "" {
		t.Error("la réponse doit contenir l'ID du job")
	}
	if job.Status != store.StatusQueued {
		t.Errorf("statut = %q, attendu %q", job.Status, store.StatusQueued)
	}

	select {
	case id := <-queue:
		if id != job.ID {
			t.Errorf("ID enfilé = %q, attendu %q", id, job.ID)
		}
	default:
		t.Error("aucun job n'a été déposé dans la file")
	}
}

func TestCreateJobInvalide(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"JSON malformé", `{"type":`},
		{"type manquant", `{"payload":{"text":"x"}}`},
		{"type vide", `{"type":""}`},
		{"champ inconnu", `{"type":"sleep","totalement_inconnu":1}`},
		{"corps vide", ``},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, h, _ := newTestServer(t)

			rec := do(h, http.MethodPost, "/jobs", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("code = %d, attendu %d", rec.Code, http.StatusBadRequest)
			}

			var resp errorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Error == "" {
				t.Errorf("la réponse doit contenir un champ \"error\", reçu: %s", rec.Body)
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	st, h, _ := newTestServer(t)
	job := st.Create("sleep", json.RawMessage(`{"ms":10}`))

	t.Run("existant", func(t *testing.T) {
		rec := do(h, http.MethodGet, "/jobs/"+job.ID, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("code = %d, attendu %d", rec.Code, http.StatusOK)
		}
		var got store.Job
		json.Unmarshal(rec.Body.Bytes(), &got)
		if got.ID != job.ID {
			t.Errorf("ID = %q, attendu %q", got.ID, job.ID)
		}
	})

	t.Run("inconnu", func(t *testing.T) {
		rec := do(h, http.MethodGet, "/jobs/nimportequoi", "")
		if rec.Code != http.StatusNotFound {
			t.Errorf("code = %d, attendu %d", rec.Code, http.StatusNotFound)
		}
	})
}

func TestListJobs(t *testing.T) {
	st, h, _ := newTestServer(t)
	a := st.Create("sleep", nil)
	st.Create("uppercase", nil)
	st.Start(a.ID, func() {})

	tests := []struct {
		name      string
		target    string
		wantCode  int
		wantCount int
	}{
		{"tous", "/jobs", http.StatusOK, 2},
		{"filtre queued", "/jobs?status=queued", http.StatusOK, 1},
		{"filtre running", "/jobs?status=running", http.StatusOK, 1},
		{"filtre sans résultat", "/jobs?status=failed", http.StatusOK, 0},
		{"filtre invalide", "/jobs?status=nawak", http.StatusBadRequest, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(h, http.MethodGet, tc.target, "")
			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, attendu %d", rec.Code, tc.wantCode)
			}
			if tc.wantCode != http.StatusOK {
				return
			}
			var resp struct {
				Count int          `json:"count"`
				Jobs  []*store.Job `json:"jobs"`
			}
			json.Unmarshal(rec.Body.Bytes(), &resp)
			if resp.Count != tc.wantCount || len(resp.Jobs) != tc.wantCount {
				t.Errorf("count = %d (%d jobs), attendu %d", resp.Count, len(resp.Jobs), tc.wantCount)
			}
		})
	}
}

func TestCancelJob(t *testing.T) {
	st, h, _ := newTestServer(t)
	job := st.Create("sleep", nil)

	rec := do(h, http.MethodDelete, "/jobs/"+job.ID, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, attendu %d", rec.Code, http.StatusOK)
	}
	var got store.Job
	json.Unmarshal(rec.Body.Bytes(), &got)
	if got.Status != store.StatusCanceled {
		t.Errorf("statut = %q, attendu %q", got.Status, store.StatusCanceled)
	}

	if rec := do(h, http.MethodDelete, "/jobs/"+job.ID, ""); rec.Code != http.StatusNotFound {
		t.Errorf("code = %d, attendu %d", rec.Code, http.StatusNotFound)
	}
}

func TestMethodeNonAutorisee(t *testing.T) {
	_, h, _ := newTestServer(t)

	rec := do(h, http.MethodPut, "/jobs", `{}`)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("code = %d, attendu %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestFileSaturee(t *testing.T) {
	st := store.New()
	queue := make(chan string, 1)
	h := NewServer(st, queue).Routes()

	if rec := do(h, http.MethodPost, "/jobs", `{"type":"sleep"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("premier POST: code = %d", rec.Code)
	}
	rec := do(h, http.MethodPost, "/jobs", `{"type":"sleep"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("code = %d, attendu %d", rec.Code, http.StatusServiceUnavailable)
	}
}
