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
	id := flag.Int("id", 0, "this node's integer ID (0-indexed)")
	cluster := flag.String("cluster", "", "comma-separated host:raftPort:httpPort for every node, e.g. localhost:7000:8000,localhost:7001:8001,localhost:7002:8002")
	flag.Parse()

	if *cluster == "" {
		fmt.Fprintln(os.Stderr, "usage: server -id N -cluster host:raftPort:httpPort,...")
		os.Exit(1)
	}

	type nodeAddr struct {
		raftAddr string
		httpAddr string
	}
	addrs := []nodeAddr{}
	for _, part := range strings.Split(*cluster, ",") {
		fields := strings.Split(strings.TrimSpace(part), ":")
		if len(fields) != 3 {
			log.Fatalf("bad cluster entry %q: want host:raftPort:httpPort", part)
		}
		addrs = append(addrs, nodeAddr{
			raftAddr: fields[0] + ":" + fields[1],
			httpAddr: fields[0] + ":" + fields[2],
		})
	}

	if *id < 0 || *id >= len(addrs) {
		log.Fatalf("id %d out of range for %d-node cluster", *id, len(addrs))
	}

	// Build the peer ID list (everyone except ourselves).
	peers := make([]int, 0, len(addrs)-1)
	for i := range addrs {
		if i != *id {
			peers = append(peers, i)
		}
	}

	// Build the HTTP peer map for leader redirects.
	httpPeers := make(map[int]string, len(addrs))
	for i, a := range addrs {
		httpPeers[i] = a.httpAddr
	}

	// Wire up the transport. HTTPTransport (coming in stage 4b) talks
	// to peers over real sockets; for now we use a placeholder that
	// panics if called, since running a real multi-process cluster
	// requires the HTTP transport wired up too.
	raftAddr := addrs[*id].raftAddr
	httpAddr := addrs[*id].httpAddr
	_ = raftAddr // will be used by HTTPTransport in the next commit

	persister := raft.NewMemoryPersister() // stage 5 replaces with FilePersister
	applyCh := make(chan raft.ApplyMsg, 256)

	ht := transport.NewHTTPTransport(*id, addrs[*id].raftAddr)
	for i, a := range addrs {
		if i != *id {
			ht.AddPeer(i, a.raftAddr)
		}
	}

	node := raft.NewNode(*id, peers, ht, persister, applyCh)
	sm := kvstore.NewStateMachine(node, applyCh)
	srv := kvstore.NewServer(sm, node, httpAddr, httpPeers)

	node.Run()
	ht.Start(node) // start serving inbound Raft RPCs

	log.Printf("node %d started | raft=%s http=%s peers=%s",
		*id, raftAddr, httpAddr,
		strings.Join(func() []string {
			s := make([]string, len(peers))
			for i, p := range peers {
				s[i] = strconv.Itoa(p)
			}
			return s
		}(), ","))

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
