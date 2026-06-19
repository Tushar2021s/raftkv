package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Tushar2021s/raftkv/raft"
)

// HTTPTransport implements raft.Transport over plain HTTP+JSON. Every
// Raft RPC becomes a POST to /raft/{rpc-name} on the target peer.
// There's deliberately no gRPC or protobuf here -- JSON over HTTP is
// enough for a correct implementation, and keeping it simple means the
// network protocol is readable with a plain curl command.
type HTTPTransport struct {
	mu     sync.RWMutex
	selfID int
	listen string // "host:port" this node listens on for inbound Raft RPCs
	peers  map[int]string
	client *http.Client
}

func NewHTTPTransport(selfID int, listen string) *HTTPTransport {
	return &HTTPTransport{
		selfID: selfID,
		listen: listen,
		peers:  make(map[int]string),
		client: &http.Client{Timeout: 200 * time.Millisecond},
	}
}

func (t *HTTPTransport) AddPeer(id int, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = addr
}

// Start registers the Raft RPC HTTP handlers and begins listening.
// node is the raft.Node whose Handle* methods we expose.
func (t *HTTPTransport) Start(node *raft.Node) {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/requestvote", func(w http.ResponseWriter, r *http.Request) {
		var args raft.RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandleRequestVote(&args)
		writeJSON(w, reply)
	})
	mux.HandleFunc("/raft/appendentries", func(w http.ResponseWriter, r *http.Request) {
		var args raft.AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandleAppendEntries(&args)
		writeJSON(w, reply)
	})
	mux.HandleFunc("/raft/installsnapshot", func(w http.ResponseWriter, r *http.Request) {
		var args raft.InstallSnapshotArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandleInstallSnapshot(&args)
		writeJSON(w, reply)
	})

	mux.HandleFunc("/raft/add-server", func(w http.ResponseWriter, r *http.Request) {
		var args raft.AddServerArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.AddServer(args.NewPeerID)
		writeJSON(w, reply)
	})
	mux.HandleFunc("/raft/remove-server", func(w http.ResponseWriter, r *http.Request) {
		var args raft.RemoveServerArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.RemoveServer(args.PeerID)
		writeJSON(w, reply)
	})

	srv := &http.Server{Addr: t.listen, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// transport errors are fatal -- without a working Raft
			// transport this node can't participate at all
			panic(fmt.Sprintf("HTTPTransport: %v", err))
		}
	}()
}

func (t *HTTPTransport) SendRequestVote(peerID int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	if err := t.post(peerID, "/raft/requestvote", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) SendAppendEntries(peerID int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	if err := t.post(peerID, "/raft/appendentries", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) SendInstallSnapshot(peerID int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	var reply raft.InstallSnapshotReply
	if err := t.post(peerID, "/raft/installsnapshot", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) post(peerID int, path string, body, reply interface{}) error {
	t.mu.RLock()
	addr, ok := t.peers[peerID]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("transport: unknown peer %d", peerID)
	}

	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := t.client.Post(
		fmt.Sprintf("http://%s%s", addr, path),
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(reply)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (t *HTTPTransport) SendAddServer(peerID int, args *raft.AddServerArgs) (*raft.AddServerReply, error) {
	var reply raft.AddServerReply
	if err := t.post(peerID, "/raft/add-server", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) SendRemoveServer(peerID int, args *raft.RemoveServerArgs) (*raft.RemoveServerReply, error) {
	var reply raft.RemoveServerReply
	if err := t.post(peerID, "/raft/remove-server", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}
