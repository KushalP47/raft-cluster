package main

import (
    "encoding/json"
    "fmt"
    "io"
    "log"
    "os"
    "path/filepath"
    "sync"

    "github.com/hashicorp/raft"
)

type StateMachine struct {
    mu       sync.Mutex
    state    map[string]string // Simple key-value store
    dataDir  string            // Directory to store log.txt and snapshot.json
}

func NewStateMachine(dataDir string) *StateMachine {
    return &StateMachine{
        state:   make(map[string]string),
        dataDir: dataDir,
    }
}

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
        fmt.Printf("Applied command: %s = %s", key, value)
    }

    // Dump the state to log.txt and snapshot.json
    if err := sm.dumpStateToFiles(); err != nil {
        fmt.Printf("Failed to dump state to files: %v", err)
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