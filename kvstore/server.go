package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/Tushar2021s/raftkv/raft"
)

// Server wraps a StateMachine and exposes it over HTTP.
//
// API:
//   GET    /kv/{key}         → 200 + {"value":"..."} | 404
//   PUT    /kv/{key}         → body: {"value":"...", "reqId":"..."} → 200 | 409 (timeout)
//   DELETE /kv/{key}         → body: {"reqId":"..."} → 200 | 409 (timeout)
//   GET    /status            → 200 + {"nodeId":N,"state":"Leader","term":T,"leader":L}
//
// When a write lands on a Follower, the server replies 307 Temporary
// Redirect pointing at the current leader's address. Clients that
// follow redirects automatically will transparently hit the right node.
type Server struct {
	sm      *StateMachine
	node    *raft.Node
	peers   map[int]string // nodeID -> "host:port" for redirect
	addr    string
}

func NewServer(sm *StateMachine, node *raft.Node, addr string, peers map[int]string) *Server {
	return &Server{sm: sm, node: node, addr: addr, peers: peers}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)
	log.Printf("node %d listening on %s", s.node.ID(), s.addr)
	return http.ListenAndServe(s.addr, mux)
}

// handleKV routes GET / PUT / DELETE for /kv/{key}
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/kv/"):]
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, _ *http.Request, key string) {
	// Reads must come from the leader so they're not stale.
	if state, _ := s.node.State(); state != raft.Leader {
		s.redirectToLeader(w)
		return
	}
	val, err := s.sm.Get(key)
	if errors.Is(err, ErrKeyMissing) {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"value": val})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	var body struct {
		Value string `json:"value"`
		ReqID string `json:"reqId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if body.ReqID == "" {
		http.Error(w, "reqId required for exactly-once delivery", http.StatusBadRequest)
		return
	}

	err := s.sm.Put(key, body.Value, body.ReqID)
	s.handleWriteResult(w, err)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	var body struct {
		ReqID string `json:"reqId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if body.ReqID == "" {
		http.Error(w, "reqId required", http.StatusBadRequest)
		return
	}

	err := s.sm.Delete(key, body.ReqID)
	s.handleWriteResult(w, err)
}

func (s *Server) handleWriteResult(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case errors.Is(err, ErrNotLeader):
		s.redirectToLeader(w)
	case errors.Is(err, ErrTimeout):
		// 409 Conflict is a reasonable choice: the client should retry
		// with the same reqId (idempotent) after checking who's leader.
		http.Error(w, "timed out waiting for commit", http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	state, term := s.node.State()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"nodeId": s.node.ID(),
		"state":  state.String(),
		"term":   term,
		"leader": s.node.LeaderID(),
	})
}

// redirectToLeader issues a 307 to the leader's address. If the leader
// is unknown (cluster is mid-election) we return 503 instead so the
// client knows to back off and retry.
func (s *Server) redirectToLeader(w http.ResponseWriter) {
	leaderID := s.node.LeaderID()
	addr, known := s.peers[leaderID]
	if !known {
		http.Error(w, "no known leader, retry shortly", http.StatusServiceUnavailable)
		return
	}
	http.Redirect(w, &http.Request{}, fmt.Sprintf("http://%s", addr), http.StatusTemporaryRedirect)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
