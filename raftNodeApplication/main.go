package main

import (
    "flag"
    "fmt"
    "log"
    "os"
    "path/filepath"

    "github.com/hashicorp/raft"
    "github.com/hashicorp/raft-boltdb"
)

func main() {
    // Parse command-line arguments
    port := flag.Int("port", 0, "Port for the NodeApplication")
    leader := flag.String("leader", "", "Leader address (empty if bootstrapping)")
    flag.Parse()

    if *port == 0 {
        log.Fatalf("Port is required")
    }

    // Define the absolute path for the data directory
    baseDataDir := "/Users/kushalpatel/Desktop/btep/raft-cluster/data"
    dataDir := filepath.Join(baseDataDir, fmt.Sprintf("nodeApp_%d", *port))
    if err := os.MkdirAll(dataDir, 0755); err != nil {
        log.Fatalf("Failed to create data directory: %v", err)
    }

    // Initialize Raft configuration
    config := raft.DefaultConfig()
    config.LocalID = raft.ServerID(fmt.Sprintf("node-%d", *port))

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
    address := fmt.Sprintf("127.0.0.1:%d", *port)
    transport, err := raft.NewTCPTransport(address, nil, 3, 10, nil)
    if err != nil {
        log.Fatalf("Failed to create transport: %v", err)
    }

    // Create Raft instance
    raftInstance, err := raft.NewRaft(config, nil, logStore, stableStore, snapshotStore, transport)
    if err != nil {
        log.Fatalf("Failed to create Raft instance: %v", err)
    }

    // Bootstrap or join the cluster
    if *leader == "" {
        // No leader specified, bootstrap the cluster
        configuration := raft.Configuration{
            Servers: []raft.Server{
                {
                    ID:      config.LocalID,
                    Address: raft.ServerAddress(address),
                },
            },
        }
        raftInstance.BootstrapCluster(configuration)
        log.Println("Bootstrapped the cluster as the first node")
    } else {
        // Join the cluster
        log.Printf("Joining the cluster via leader at %s\n", *leader)
        // Wait for the leader to add this node using AddVoter
    }

    // Create the ready.txt file to signal readiness
    readyFile := filepath.Join(dataDir, "ready.txt")
    if err := os.WriteFile(readyFile, []byte("ready"), 0644); err != nil {
        log.Fatalf("Failed to create ready file: %v", err)
    }
    log.Printf("NodeApplication is ready. Ready file created at %s\n", readyFile)

    // Keep the node running
    select {}
}