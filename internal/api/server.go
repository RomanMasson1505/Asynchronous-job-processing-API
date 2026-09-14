package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/RomanMasson1505/jobapi/internal/store"
)

type Server struct {
	store *store.Store
	queue chan<- string
}

func NewServer(st *store.Store, queue chan<- string) *Server {
	return &Server{store: st, queue: queue}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /jobs", s.handleCreate)
	mux.HandleFunc("GET /jobs", s.handleList)
	mux.HandleFunc("GET /jobs/{id}", s.handleGet)
	mux.HandleFunc("DELETE /jobs/{id}", s.handleCancel)

	return logging(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type createRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req createRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "corps JSON invalide: "+err.Error())
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, `le champ "type" est obligatoire`)
		return
	}

	job := s.store.Create(req.Type, req.Payload)

	select {
	case s.queue <- job.ID:
		writeJSON(w, http.StatusAccepted, job)
	default:
		s.store.Finish(job.ID, nil, errors.New("file d'attente saturée"))
		writeError(w, http.StatusServiceUnavailable, "file d'attente saturée, réessayez plus tard")
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	job, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job introuvable")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	filter := store.Status(r.URL.Query().Get("status"))
	if filter != "" && !filter.IsValid() {
		writeError(w, http.StatusBadRequest, "statut inconnu: "+string(filter))
		return
	}

	jobs := s.store.List(filter)
	writeJSON(w, http.StatusOK, map[string]any{
		"count": len(jobs),
		"jobs":  jobs,
	})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if !s.store.Cancel(id) {
		writeError(w, http.StatusNotFound, "job introuvable ou déjà terminé")
		return
	}

	job, _ := s.store.Get(id)
	writeJSON(w, http.StatusOK, job)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)

	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("écriture de la réponse: %v", err)
	}
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, errorResponse{Error: msg})
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s (%s)", r.Method, r.URL.Path, time.Since(start).Round(time.Microsecond))
	})
}
