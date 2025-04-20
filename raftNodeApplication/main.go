package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
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

	// Redirect logs to a file named serverLogs
	logFilePath := filepath.Join(dataDir, "serverLogs")
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// Initialize Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(fmt.Sprintf("node-%d", *port))
	config.Logger = hclog.New(&hclog.LoggerOptions{
		Output: logFile,
		Level:  hclog.Debug,
		Name:   "raft",
	}) // Set Raft logger

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

	// Initialize the state machine
	stateMachine := NewStateMachine(dataDir)

	// Create Raft instance
	raftInstance, err := raft.NewRaft(config, stateMachine, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("Failed to create Raft instance: %v", err)
	}

	// Bootstrap or join the cluster
	log.Println("Starting NodeApplication with leader:", *leader)
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
		if err := raftInstance.BootstrapCluster(configuration).Error(); err != nil {
			log.Fatalf("Failed to bootstrap cluster: %v", err)
		}
		log.Println("Bootstrapped the cluster as the first node")
	} else {
		// Join the cluster
		log.Printf("Joining the cluster via leader at %s\n", *leader)
		// Wait for the leader to add this node using AddVoter
	}

	// Update leader info in leaderInfo.txt
	go func() {
		leaderInfoPath := filepath.Join(baseDataDir, "leaderInfo.txt")
		for {
			if raftInstance.State() == raft.Leader {
				// Write the leader's port to leaderInfo.txt
				if err := os.WriteFile(leaderInfoPath, []byte(fmt.Sprintf("%d", *port)), 0644); err != nil {
					log.Printf("Failed to write leader info: %v", err)
				}
			}
			time.Sleep(1 * time.Second) // Check periodically
		}
	}()

	// Create the ready.txt file to signal readiness
	readyFile := filepath.Join(dataDir, "ready.txt")
	if err := os.WriteFile(readyFile, []byte("ready"), 0644); err != nil {
		log.Fatalf("Failed to create ready file: %v", err)
	}
	log.Printf("NodeApplication is ready. Ready file created at %s\n", readyFile)

	// Expose APIs
	http.HandleFunc("/addNode", func(w http.ResponseWriter, r *http.Request) {
		if raftInstance.State() != raft.Leader {
			http.Error(w, "Not the leader", http.StatusForbidden)
			return
		}

		portStr := r.URL.Query().Get("port")
		port, err := strconv.Atoi(portStr)
		if err != nil {
			http.Error(w, "Invalid port", http.StatusBadRequest)
			return
		}

		addVoterFuture := raftInstance.AddVoter(
			raft.ServerID(fmt.Sprintf("node-%d", port)),
			raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", port)),
			0, // Index
			0, // Timeout
		)
		if err := addVoterFuture.Error(); err != nil {
			http.Error(w, fmt.Sprintf("Failed to add node: %v", err), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "Node %d added successfully", port)
	})

	http.HandleFunc("/removeNode", func(w http.ResponseWriter, r *http.Request) {
		if raftInstance.State() != raft.Leader {
			http.Error(w, "Not the leader", http.StatusForbidden)
			return
		}

		portStr := r.URL.Query().Get("port")
		port, err := strconv.Atoi(portStr)
		if err != nil {
			http.Error(w, "Invalid port", http.StatusBadRequest)
			return
		}

		removeServerFuture := raftInstance.RemoveServer(
			raft.ServerID(fmt.Sprintf("node-%d", port)),
			0, // Index
			0, // Timeout
		)
		if err := removeServerFuture.Error(); err != nil {
			http.Error(w, fmt.Sprintf("Failed to remove node: %v", err), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "Node %d removed successfully", port)
	})

	http.HandleFunc("/clientRequest", func(w http.ResponseWriter, r *http.Request) {
		if raftInstance.State() != raft.Leader {
			http.Error(w, "Not the leader", http.StatusForbidden)
			return
		}

		var command map[string]string
		if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		data, err := json.Marshal(command)
		if err != nil {
			http.Error(w, "Failed to marshal command", http.StatusInternalServerError)
			return
		}

		applyFuture := raftInstance.Apply(data, 10*time.Second)
		if err := applyFuture.Error(); err != nil {
			http.Error(w, fmt.Sprintf("Failed to apply command: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "Command applied successfully")
	})

	// Start the HTTP server
	go func() {
		log.Printf("Starting HTTP server on port %d", *port+1000) // HTTP server on port offset by 1000
		if err := http.ListenAndServe(fmt.Sprintf(":%d", *port+1000), nil); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	// Keep the node running
	select {}
}

type StateMachine struct {
	mu      sync.Mutex
	state   map[string]string // Simple key-value store
	dataDir string            // Directory to store log.txt and snapshot.json
}

func NewStateMachine(dataDir string) *StateMachine {
	return &StateMachine{
		state:   make(map[string]string),
		dataDir: dataDir,
	}
}

// Apply applies a committed log entry to the state machine.
// Apply applies a committed log entry to the state machine.
func (sm *StateMachine) Apply(log *raft.Log) interface{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var command map[string]string
	if err := json.Unmarshal(log.Data, &command); err != nil {
		fmt.Printf("Failed to unmarshal log entry: %v", err)
		return nil
	}

	// Apply the command to the state
	for key, value := range command {
		sm.state[key] = value
		fmt.Printf("Applied command: %s = %s\n", key, value)
	}

	// Dump the state to log.txt and snapshot.json
	if err := sm.dumpStateToFiles(); err != nil {
		fmt.Printf("Failed to dump state to files: %v\n", err)
	}

	return nil
}

// Snapshot creates a snapshot of the current state.
func (sm *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	data, err := json.Marshal(sm.state)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %v", err)
	}

	return &Snapshot{data: data}, nil
}

// Restore restores the state from a snapshot.
func (sm *StateMachine) Restore(rc io.ReadCloser) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to restore snapshot: %v", err)
	}

	if err := json.Unmarshal(data, &sm.state); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %v", err)
	}

	log.Println("State restored from snapshot")
	return nil
}

// dumpStateToFiles writes the current state to log.txt and snapshot.json in the data directory.
func (sm *StateMachine) dumpStateToFiles() error {
	// Write to log.txt
	logFilePath := filepath.Join(sm.dataDir, "log.txt")
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log.txt: %v", err)
	}
	defer logFile.Close()

	for key, value := range sm.state {
		if _, err := logFile.WriteString(fmt.Sprintf("%s = %s\n", key, value)); err != nil {
			return fmt.Errorf("failed to write to log.txt: %v", err)
		}
	}

	// Write to snapshot.json
	snapshotFilePath := filepath.Join(sm.dataDir, "snapshot.json")
	snapshotData, err := json.MarshalIndent(sm.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state to JSON: %v", err)
	}

	if err := os.WriteFile(snapshotFilePath, snapshotData, 0644); err != nil {
		return fmt.Errorf("failed to write to snapshot.json: %v", err)
	}

	log.Println("State successfully dumped to log.txt and snapshot.json")
	return nil
}

type Snapshot struct {
	data []byte
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *Snapshot) Release() {}
