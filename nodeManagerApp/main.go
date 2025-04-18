package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/KushalP47/raft-cluster/nodeManagerApp/handlers"
	"github.com/KushalP47/raft-cluster/nodeManagerApp/manager"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

func main() {
	// Initialize Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID("mainapp")

	// Create data directories
	dataDir := filepath.Join("data", "mainapp")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize Raft storage
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.db"))
	if err != nil {
		log.Fatalf("Failed to create log store: %v", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.db"))
	if err != nil {
		log.Fatalf("Failed to create stable store: %v", err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "snapshots"), 3, nil)
	if err != nil {
		log.Fatalf("Failed to create snapshot store: %v", err)
	}

	// Initialize Raft transport
	transport, err := raft.NewTCPTransport("127.0.0.1:8000", nil, 3, 10, nil)
	if err != nil {
		log.Fatalf("Failed to create transport: %v", err)
	}

	// Create Raft instance
	raftInstance, err := raft.NewRaft(config, nil, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("Failed to create Raft instance: %v", err)
	}

	// Initialize NodeManager
	nodeManager := manager.NewNodeManager(raftInstance)

	// Register API handlers
	http.HandleFunc("/addNode", handlers.AddNodeHandler(nodeManager))
	http.HandleFunc("/stopNode", handlers.StopNodeHandler(nodeManager))
	http.HandleFunc("/clientRequest", handlers.ClientRequestHandler(nodeManager))

	log.Println("Starting main application on port 8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
