package storage

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"
)

type Command struct {
    Operation string      `json:"op"`
    Key       string      `json:"key,omitempty"`
    Value     any `json:"value,omitempty"`
    Timestamp time.Time   `json:"timestamp"`
}

type Storage struct {
    port      int
    dataPath  string
    logPath   string
    mu        sync.Mutex
}

func NewStorage(port int) *Storage {
    dataDir := filepath.Join("data", fmt.Sprintf("node_%d.json", port))
    logDir := filepath.Join("logs", fmt.Sprintf("node_%d.log", port))

    // Ensure directories exist
    if err := os.MkdirAll(filepath.Dir(dataDir), 0755); err != nil {
        panic(fmt.Sprintf("Failed to create data directory: %v", err))
    }
    if err := os.MkdirAll(filepath.Dir(logDir), 0755); err != nil {
        panic(fmt.Sprintf("Failed to create logs directory: %v", err))
    }

    return &Storage{
        port:     port,
        dataPath: dataDir,
        logPath:  logDir,
    }
}

// AppendCommand writes a new command to both JSON store and log file
func (s *Storage) AppendCommand(cmd *Command) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    cmd.Timestamp = time.Now().UTC()

    // Append to JSON file
    if err := s.appendToJSON(cmd); err != nil {
        return err
    }

    // Write to log file
    if err := s.appendToLog(cmd); err != nil {
        return err
    }

    return nil
}

func (s *Storage) appendToJSON(cmd *Command) error {
    // Read existing commands
    commands, err := s.GetAllCommands()
    if err != nil {
        return err
    }

    // Append new command
    commands = append(commands, *cmd)

    // Write back to file
    data, err := json.MarshalIndent(commands, "", "  ")
    if err != nil {
        return fmt.Errorf("json marshaling error: %v", err)
    }

    return os.WriteFile(s.dataPath, data, 0644)
}

func (s *Storage) appendToLog(cmd *Command) error {
    logEntry := fmt.Sprintf("[%s] %s %s: %v\n",
        cmd.Timestamp.Format(time.RFC3339),
        cmd.Operation,
        cmd.Key,
        cmd.Value,
    )

    f, err := os.OpenFile(s.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return fmt.Errorf("error opening log file: %v", err)
    }
    defer f.Close()

    if _, err := f.WriteString(logEntry); err != nil {
        return fmt.Errorf("error writing to log file: %v", err)
    }
    return nil
}

// GetAllCommands reads all commands from the JSON file
func (s *Storage) GetAllCommands() ([]Command, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    var commands []Command

    data, err := os.ReadFile(s.dataPath)
    if err != nil {
        if os.IsNotExist(err) {
            return []Command{}, nil
        }
        return nil, fmt.Errorf("error reading data file: %v", err)
    }

    if len(data) == 0 {
        return []Command{}, nil
    }

    if err := json.Unmarshal(data, &commands); err != nil {
        return nil, fmt.Errorf("error unmarshaling data: %v", err)
    }

    return commands, nil
}

// GetCommandByKey searches the JSON store for a specific key
func (s *Storage) GetCommandByKey(key string) (*Command, error) {
    commands, err := s.GetAllCommands()
    if err != nil {
        return nil, err
    }

    for _, cmd := range commands {
        if cmd.Key == key {
            return &cmd, nil
        }
    }

    return nil, fmt.Errorf("key not found: %s", key)
}

// CompactLog rotates log files when they reach 1MB
func (s *Storage) CompactLog() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    info, err := os.Stat(s.logPath)
    if err != nil {
        return err
    }

    if info.Size() > 1<<20 { // 1MB
        newPath := fmt.Sprintf("%s.%d.bak", s.logPath, time.Now().Unix())
        return os.Rename(s.logPath, newPath)
    }

    return nil
}
