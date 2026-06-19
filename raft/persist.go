package raft

import "encoding/json"

// persistentState is everything that must survive a crash: the Raft
// paper's Figure 2 fields plus snapshot metadata (stage 6) plus the
// cluster configuration (stage 7). Without the config, a restarted
// node wouldn't know which peers to contact or whether it's in the
// middle of a membership transition.
//
//	type persistentState struct {
//		CurrentTerm       int           `json:"currentTerm"`
//		VotedFor          int           `json:"votedFor"`
//		Log               []LogEntry    `json:"log"`
//		LastIncludedIndex int           `json:"lastIncludedIndex"`
//		LastIncludedTerm  int           `json:"lastIncludedTerm"`
//		ClusterConfig     ClusterConfig `json:"clusterConfig"`
//	}
type persistentState struct {
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    int        `json:"votedFor"`
	Log         []LogEntry `json:"log"`

	LastIncludedIndex int `json:"lastIncludedIndex"`
	LastIncludedTerm  int `json:"lastIncludedTerm"`

	ConfigStable ClusterConfig `json:"configStable"`
	ConfigOld    ClusterConfig `json:"configOld"`
	ConfigNew    ClusterConfig `json:"configNew"`

	ConfigJoint        bool `json:"configJoint"`
	PendingConfigIndex int  `json:"pendingConfigIndex"`
}

func (n *Node) persistLocked() {
	data, err := json.Marshal(persistentState{
		CurrentTerm:        n.currentTerm,
		VotedFor:           n.votedFor,
		Log:                n.log,
		LastIncludedIndex:  n.lastIncludedIndex,
		LastIncludedTerm:   n.lastIncludedTerm,
		ConfigStable:       n.configStable,
		ConfigOld:          n.configOld,
		ConfigNew:          n.configNew,
		ConfigJoint:        n.configJoint,
		PendingConfigIndex: n.pendingConfigIndex,
	})
	if err != nil {
		panic(err)
	}
	_ = n.persister.SaveState(data)
}

func (n *Node) restoreLocked() {
	data, err := n.persister.ReadState()
	if err != nil || len(data) == 0 {
		return
	}
	var ps persistentState
	if err := json.Unmarshal(data, &ps); err != nil {
		return
	}
	n.currentTerm = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	n.log = ps.Log
	n.lastIncludedIndex = ps.LastIncludedIndex
	n.lastIncludedTerm = ps.LastIncludedTerm
	// if len(ps.ClusterConfig.NewPeers) > 0 {
	// 	n.clusterConfig = ps.ClusterConfig
	// 	n.peers = allPeersLocked(&ps.ClusterConfig, n.id)
	// }
	n.configStable = ps.ConfigStable
	n.configOld = ps.ConfigOld
	n.configNew = ps.ConfigNew
	n.configJoint = ps.ConfigJoint
	n.pendingConfigIndex = ps.PendingConfigIndex
	n.commitIndex = ps.LastIncludedIndex
	n.lastApplied = ps.LastIncludedIndex
	if n.configJoint {
		union := dedupSorted(
			append(
				append([]int{}, n.configOld.Members...),
				n.configNew.Members...,
			),
		)

		n.peers = without(union, n.id)
	} else {
		n.peers = without(
			n.configStable.Members,
			n.id,
		)
	}
}
