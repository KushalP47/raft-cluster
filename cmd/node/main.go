package main

import (
    "github.com/hashicorp/raft"
    "raft-cluster/internal/fsm"
)

func initRaftNode(port int, leader string) (*raft.Raft, error) {
    config := raft.DefaultConfig()
    config.LocalID = raft.ServerID(fmt.Sprintf("node-%d", port))
    
    // Set up network transport
    addr := fmt.Sprintf("127.0.0.1:%d", port)
    transport, _ := raft.NewTCPTransport(addr, nil, 3, 10*time.Second, os.Stderr)
    
    // Set up FSM with JSON storage
    fsm := fsm.NewJSONFSM(port)
    
    // Initialize Raft
    return raft.NewRaft(
        config,
        fsm,
        raft.NewInmemStore(), // Temporary in-memory store
        raft.NewInmemStore(),
        raft.NewInmemSnapshotStore(),
        transport,
    )
}

