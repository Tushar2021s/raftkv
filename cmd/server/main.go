package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/Tushar2021s/raftkv/kvstore"
	"github.com/Tushar2021s/raftkv/raft"
	"github.com/Tushar2021s/raftkv/transport"
)

func main() {
	id      := flag.Int("id", 0, "this node's integer ID (0-indexed)")
	cluster := flag.String("cluster", "",
		"comma-separated host:raftPort:httpPort for every node, "+
			"e.g. localhost:7000:8000,localhost:7001:8001,localhost:7002:8002")
	dataDir := flag.String("data", "", "directory for persistent state (default: ./data/node<id>)")
	flag.Parse()

	if *cluster == "" {
		fmt.Fprintln(os.Stderr, "usage: server -id N -cluster host:raftPort:httpPort,...")
		os.Exit(1)
	}

	type nodeAddr struct{ raftAddr, httpAddr string }
	var addrs []nodeAddr
	for _, part := range strings.Split(*cluster, ",") {
		f := strings.Split(strings.TrimSpace(part), ":")
		if len(f) != 3 {
			log.Fatalf("bad cluster entry %q: want host:raftPort:httpPort", part)
		}
		addrs = append(addrs, nodeAddr{f[0] + ":" + f[1], f[0] + ":" + f[2]})
	}
	if *id < 0 || *id >= len(addrs) {
		log.Fatalf("id %d out of range for %d-node cluster", *id, len(addrs))
	}

	peers := make([]int, 0, len(addrs)-1)
	for i := range addrs {
		if i != *id {
			peers = append(peers, i)
		}
	}
	httpPeers := make(map[int]string, len(addrs))
	for i, a := range addrs {
		httpPeers[i] = a.httpAddr
	}

	// --- Persistence ---
	dir := *dataDir
	if dir == "" {
		dir = fmt.Sprintf("data/node%d", *id)
	}
	persister, err := raft.NewFilePersister(dir)
	if err != nil {
		log.Fatalf("failed to create persister at %s: %v", dir, err)
	}
	log.Printf("node %d: persisting state to %s", *id, dir)

	// --- Transport ---
	ht := transport.NewHTTPTransport(*id, addrs[*id].raftAddr)
	for i, a := range addrs {
		if i != *id {
			ht.AddPeer(i, a.raftAddr)
		}
	}

	// --- Raft node ---
	applyCh := make(chan raft.ApplyMsg, 256)
	node := raft.NewNode(*id, peers, ht, persister, applyCh)
	sm   := kvstore.NewStateMachine(node, applyCh)
	srv  := kvstore.NewServer(sm, node, addrs[*id].httpAddr, httpPeers)

	node.Run()
	ht.Start(node)

	log.Printf("node %d started | raft=%s http=%s peers=%s",
		*id, addrs[*id].raftAddr, addrs[*id].httpAddr,
		strings.Join(func() []string {
			s := make([]string, len(peers))
			for i, p := range peers {
				s[i] = strconv.Itoa(p)
			}
			return s
		}(), ","),
	)

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
