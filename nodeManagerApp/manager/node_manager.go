package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus" // Structured logging library
)

type NodeState struct {
    Port     int  `json:"port"`
    Status   bool `json:"status"`   // true = alive, false = dead
    IsLeader bool `json:"isLeader"` // true = leader, false = follower
}

type NodeManager struct {
    mu       sync.RWMutex
    nodes    map[int]*NodeState
    jsonFile string
    raft     *raft.Raft // Reference to the Raft instance
    logger   *logrus.Logger
    baseDataDir string
}

// NewNodeManager initializes the NodeManager with all ports (8001–8025) set to dead.
func NewNodeManager(raftInstance *raft.Raft) *NodeManager {
    logger := logrus.New()
    logger.SetFormatter(&logrus.JSONFormatter{}) // Use JSON logging for better structure

    manager := &NodeManager{
        nodes:    make(map[int]*NodeState),
        jsonFile: "node_manager_state.json",
        raft:     raftInstance,
        logger:   logger,
    }

    manager.baseDataDir = "/Users/kushalpatel/Desktop/btep/raft-cluster/data" // Absolute path for data directory

    // Initialize all ports with default state
    for port := 8001; port <= 8025; port++ {
        manager.nodes[port] = &NodeState{
            Port:     port,
            Status:   false,
            IsLeader: false,
        }
    }

    // Load state from JSON file if it exists
    manager.loadStateFromFile()

    return manager
}

// AddNode adds a node to the Raft cluster and updates its state.
func (nm *NodeManager) AddNode(port int, isLeader bool) error {
    nm.mu.RLock()
    node, exists := nm.nodes[port]
    nm.mu.RUnlock()

    if !exists {
        return fmt.Errorf("port %d is out of range (8001–8025)", port)
    }

    // Ensure Raft instance is initialized
    if err := nm.ensureRaftInitialized(); err != nil {
        nm.logger.WithError(err).Error("Raft instance is not initialized")
        return err
    }

    // Determine the leader
    leader, err := nm.GetLeader()
    var leaderAddr string
    if err != nil {
        nm.logger.Warn("No leader found, bootstrapping the cluster")
        leaderAddr = "" // No leader exists, bootstrap the cluster
    } else {
        leaderAddr = fmt.Sprintf("127.0.0.1:%d", leader)
    }

    // Start the NodeApplication server
    cmd := exec.Command("go", "run", "raftNodeApplication/main.go", fmt.Sprintf("--port=%d", port), fmt.Sprintf("--leader=%s", leaderAddr))
    if err := cmd.Start(); err != nil {
        nm.logger.WithFields(logrus.Fields{
            "port": port,
        }).WithError(err).Error("Failed to start NodeApplication server")
        return fmt.Errorf("failed to start NodeApplication server: %v", err)
    }
    nm.logger.WithFields(logrus.Fields{
        "port":       port,
        "leaderAddr": leaderAddr,
    }).Info("NodeApplication server started successfully")

    // Wait for the ready.txt file to be created
    readyFile := filepath.Join(nm.baseDataDir, fmt.Sprintf("nodeApp_%d", port), "ready.txt")
    for i := 0; i < 4; i++ { // Retry up to 4 times (5-second intervals)
        if _, err := os.Stat(readyFile); err == nil {
            nm.logger.WithFields(logrus.Fields{
                "port": port,
            }).Info("NodeApplication server is ready")
            break
        }
        time.Sleep(5 * time.Second) // Wait 5 seconds before retrying
    }

    // Check if the ready file exists after retries
    if _, err := os.Stat(readyFile); err != nil {
        nm.logger.WithFields(logrus.Fields{
            "port": port,
        }).Error("NodeApplication server did not become ready in time")
        return fmt.Errorf("NodeApplication server did not become ready in time")
    }

    // Add the node to the Raft cluster
    future := nm.raft.AddVoter(raft.ServerID(fmt.Sprintf("node-%d", port)), raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", port)), 0, 0)
    if err := future.Error(); err != nil {
        nm.logger.WithFields(logrus.Fields{
            "port": port,
        }).WithError(err).Error("Failed to add node to Raft cluster")
        return fmt.Errorf("failed to add node to Raft cluster: %v", err)
    }

    // Update the node state
    nm.mu.Lock()
    node.Status = true
    node.IsLeader = isLeader
    nm.saveStateToFile()
    nm.mu.Unlock()

    nm.logger.WithFields(logrus.Fields{
        "port":     port,
        "isLeader": isLeader,
    }).Info("Node added successfully")
    return nil
}

// RemoveNode removes a node from the Raft cluster and updates its state.
func (nm *NodeManager) RemoveNode(port int) error {
    nm.mu.RLock()
    node, exists := nm.nodes[port]
    nm.mu.RUnlock()

    if !exists {
        return fmt.Errorf("port %d is out of range (8001–8025)", port)
    }

    // Ensure Raft instance is initialized
    if err := nm.ensureRaftInitialized(); err != nil {
        nm.logger.WithError(err).Error("Raft instance is not initialized")
        return err
    }

    // Mark the node as not ready by deleting the ready.txt file
    readyFile := filepath.Join(nm.baseDataDir, fmt.Sprintf("nodeApp_%d", port), "ready.txt")
    if err := os.Remove(readyFile); err != nil && !os.IsNotExist(err) {
        nm.logger.WithFields(logrus.Fields{
            "port": port,
        }).WithError(err).Error("Failed to delete ready file")
        return fmt.Errorf("failed to delete ready file: %v", err)
    }
    nm.logger.WithFields(logrus.Fields{
        "port": port,
    }).Info("Ready file deleted successfully")

    // Stop the NodeApplication server
    cmd := exec.Command("lsof", "-t", fmt.Sprintf("-i:%d", port)) // Find the process ID (PID) using the port
    output, err := cmd.Output()
    if err != nil {
        nm.logger.WithFields(logrus.Fields{
            "port": port,
        }).WithError(err).Error("Failed to find NodeApplication server process")
        return fmt.Errorf("failed to find NodeApplication server process: %v", err)
    }

    pid := strings.TrimSpace(string(output))
    killCmd := exec.Command("kill", "-9", pid) // Kill the process
    if err := killCmd.Run(); err != nil {
        nm.logger.WithFields(logrus.Fields{
            "port": port,
        }).WithError(err).Error("Failed to stop NodeApplication server")
        return fmt.Errorf("failed to stop NodeApplication server: %v", err)
    }
    nm.logger.WithFields(logrus.Fields{
        "port": port,
    }).Info("NodeApplication server stopped successfully")

    // Remove the node from the Raft cluster
    future := nm.raft.RemoveServer(raft.ServerID(fmt.Sprintf("node-%d", port)), 0, 0)
    if err := future.Error(); err != nil {
        nm.logger.WithFields(logrus.Fields{
            "port": port,
        }).WithError(err).Error("Failed to remove node from Raft cluster")
        return fmt.Errorf("failed to remove node from Raft cluster: %v", err)
    }

    // Update the node state
    nm.mu.Lock()
    node.Status = false
    node.IsLeader = false
    nm.saveStateToFile()
    nm.mu.Unlock()

    nm.logger.WithFields(logrus.Fields{
        "port": port,
    }).Info("Node removed successfully")
    return nil
}

// GetLeader dynamically determines the leader node using the Raft instance.
func (nm *NodeManager) GetLeader() (int, error) {
    nm.mu.RLock()
    defer nm.mu.RUnlock()

    leaderAddr := nm.raft.Leader()
    if leaderAddr == "" {
        nm.logger.Warn("No leader found in the cluster")
        return 0, fmt.Errorf("no leader found")
    }

    // Find the node with the matching address
    for _, node := range nm.nodes {
        if fmt.Sprintf("127.0.0.1:%d", node.Port) == string(leaderAddr) && node.Status {
            node.IsLeader = true
            return node.Port, nil
        }
    }
    nm.logger.Warn("Leader address does not match any known node")
    return 0, fmt.Errorf("leader address does not match any known node")
}

// GetNodeState returns the state of a specific node.
func (nm *NodeManager) GetNodeState(port int) (*NodeState, error) {
    nm.mu.RLock()
    defer nm.mu.RUnlock()

    if node, exists := nm.nodes[port]; exists {
        return node, nil
    }
    return nil, fmt.Errorf("port %d is out of range (8001–8025)", port)
}

// Save the current state to a JSON file.
func (nm *NodeManager) saveStateToFile() {
    backupFile := nm.jsonFile + ".bak"
    os.Rename(nm.jsonFile, backupFile) // Create a backup

    file, err := os.Create(nm.jsonFile)
    if err != nil {
        nm.logger.WithError(err).Error("Failed to save state to file")
        return
    }
    defer file.Close()

    encoder := json.NewEncoder(file)
    encoder.SetIndent("", "  ")
    if err := encoder.Encode(nm.nodes); err != nil {
        nm.logger.WithError(err).Error("Failed to encode state to JSON")
        os.Rename(backupFile, nm.jsonFile) // Restore backup
    }
}

// Load the state from a JSON file.
func (nm *NodeManager) loadStateFromFile() {
    file, err := os.Open(nm.jsonFile)
    if err != nil {
        // File might not exist on the first run; this is fine.
        nm.logger.WithError(err).Warn("State file not found, starting with default state")
        return
    }
    defer file.Close()

    decoder := json.NewDecoder(file)
    if err := decoder.Decode(&nm.nodes); err != nil {
        nm.logger.WithError(err).Error("Failed to decode state from JSON")
    }
}

// Ensure Raft instance is initialized before performing operations.
func (nm *NodeManager) ensureRaftInitialized() error {
    if nm.raft == nil {
        return fmt.Errorf("raft instance is not initialized")
    }
    return nil
}