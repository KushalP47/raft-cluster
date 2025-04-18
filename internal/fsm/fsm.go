package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type Command struct {
	Operation string      `json:"op"`
	Key       string      `json:"key,omitempty"`
	Value     interface{} `json:"value,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

type JSONFSM struct {
	port     int
	dataFile string
	logFile  string
	mu       sync.Mutex
}

func NewJSONFSM(port int) *JSONFSM {
	dataDir := filepath.Join("data", fmt.Sprintf("node_%d.json", port))
	logDir := filepath.Join("logs", fmt.Sprintf("node_%d.log", port))

	// Ensure data directory exists
	if err := os.MkdirAll(filepath.Dir(dataDir), 0755); err != nil {
		panic(fmt.Sprintf("Failed to create data directory: %v", err))
	}

	return &JSONFSM{
		port:     port,
		dataFile: dataDir,
		logFile:  logDir,
	}
}

// Apply log entry to the FSM
func (f *JSONFSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %v", err)
	}
	cmd.Timestamp = time.Now().UTC()

	// Append to JSON file
	if err := f.appendToFile(cmd); err != nil {
		return err
	}

	// Write to log file
	if err := f.writeLogEntry(cmd); err != nil {
		return err
	}

	return nil
}

func (f *JSONFSM) appendToFile(cmd Command) error {
	// Read existing data
	var commands []Command
	data, _ := os.ReadFile(f.dataFile)
	if len(data) > 0 {
		if err := json.Unmarshal(data, &commands); err != nil {
			return err
		}
	}

	// Append new command
	commands = append(commands, cmd)

	// Write back to file
	newData, err := json.MarshalIndent(commands, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(f.dataFile, newData, 0644)
}

func (f *JSONFSM) writeLogEntry(cmd Command) error {
	logEntry := fmt.Sprintf("[%s] %s %s: %v\n",
		cmd.Timestamp.Format(time.RFC3339),
		cmd.Operation,
		cmd.Key,
		cmd.Value,
	)

	logFile, err := os.OpenFile(f.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer func() {
		if closeErr := logFile.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close log file: %w", closeErr)
		}
	}()

	if _, writeErr := logFile.WriteString(logEntry); writeErr != nil {
		return fmt.Errorf("failed to write log entry: %w", writeErr)
	}

	return nil
}

// Snapshot returns a snapshot of the current state
func (f *JSONFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.dataFile)
	if err != nil {
		return nil, err
	}

	return &snapshot{data: data}, nil
}

// Restore restores the FSM from a snapshot
func (f *JSONFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var commands []Command
	if err := json.NewDecoder(rc).Decode(&commands); err != nil {
		return err
	}

	newData, err := json.MarshalIndent(commands, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(f.dataFile, newData, 0644)
}

type snapshot struct {
	data []byte
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
