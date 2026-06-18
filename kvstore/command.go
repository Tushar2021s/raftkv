package kvstore

import "encoding/json"

// OpType identifies which KV operation a log entry represents.
type OpType string

const (
	OpPut    OpType = "PUT"
	OpDelete OpType = "DELETE"
	// GET is intentionally NOT a log entry -- reads are served directly
	// from the state machine on the leader without going through Raft.
	// Linearizable reads need a lease or ReadIndex protocol (a later
	// optimisation); for now we serve reads from the leader only, which
	// gives us sequential consistency cheaply.
)

// Command is what gets JSON-encoded into a raft.LogEntry.Command.
// Raft itself treats it as opaque bytes and has no idea what it means.
type Command struct {
	Op    OpType `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"` // empty for DELETE
	// ReqID lets the state machine deduplicate retried client requests:
	// if a client retries a PUT after a timeout (it didn't know whether
	// the first attempt committed), the state machine can recognise the
	// same ReqID and return the cached result instead of applying twice.
	ReqID string `json:"reqId"`
}

func encodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func decodeCommand(data []byte) (Command, error) {
	var cmd Command
	return cmd, json.Unmarshal(data, &cmd)
}
