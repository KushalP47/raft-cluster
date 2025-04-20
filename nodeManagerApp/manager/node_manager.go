package manager

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type NodeState struct {
	Port     int  `json:"port"`
	Status   bool `json:"status"`   // true = alive, false = dead
	IsLeader bool `json:"isLeader"` // true = leader, false = follower
}

type NodeManager struct {
	mu          sync.RWMutex
	nodes       map[int]*NodeState
	jsonFile    string
	logger      *logrus.Logger
	baseDataDir string
}

// NewNodeManager initializes the NodeManager with all ports (8001–8025) set to dead.
func NewNodeManager() *NodeManager {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{}) // Use JSON logging for better structure

	manager := &NodeManager{
		nodes:       make(map[int]*NodeState),
		jsonFile:    "node_manager_state.json",
		logger:      logger,
		baseDataDir: "/Users/kushalpatel/Desktop/btep/raft-cluster/data", // Absolute path for data directory
	}

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

	// Get the leader's port
	leaderPort, err := nm.GetLeader()
	if err != nil {
		return fmt.Errorf("failed to get leader: %v", err)
	}

	// Start the NodeApplication server with the leader's address
	var cmd *exec.Cmd
	if isLeader {
		// Bootstrap the first node as the leader
		cmd = exec.Command("go", "run", "raftNodeApplication/main.go", fmt.Sprintf("--port=%d", port))
	} else {
		// Pass the leader's address for new nodes
		leaderAddress := fmt.Sprintf("127.0.0.1:%d", leaderPort)
		cmd = exec.Command("go", "run", "raftNodeApplication/main.go", fmt.Sprintf("--port=%d", port), fmt.Sprintf("--leader=%s", leaderAddress))
	}

	if err := cmd.Start(); err != nil {
		nm.logger.WithFields(logrus.Fields{
			"port": port,
		}).WithError(err).Error("Failed to start NodeApplication server")
		return fmt.Errorf("failed to start NodeApplication server: %v", err)
	}
	nm.logger.WithFields(logrus.Fields{
		"port": port,
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

	// Send a request to the leader's /addNode API
	leaderURL := fmt.Sprintf("http://127.0.0.1:%d/addNode?port=%d", leaderPort+1000, port)
	resp, err := http.Get(leaderURL)
	if err != nil {
		nm.logger.WithFields(logrus.Fields{
			"port":       port,
			"leaderPort": leaderPort,
		}).WithError(err).Error("Failed to send addNode request to leader")
		return fmt.Errorf("failed to send addNode request to leader: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		nm.logger.WithFields(logrus.Fields{
			"port":       port,
			"leaderPort": leaderPort,
			"response":   string(body),
		}).Error("Leader failed to add node")
		return fmt.Errorf("leader failed to add node: %s", string(body))
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

	// Get the leader's port
	leaderPort, err := nm.GetLeader()
	if err != nil {
		return fmt.Errorf("failed to get leader: %v", err)
	}

	// Send a request to the leader's /removeNode API
	leaderURL := fmt.Sprintf("http://127.0.0.1:%d/removeNode?port=%d", leaderPort+1000, port)
	resp, err := http.Get(leaderURL)
	if err != nil {
		nm.logger.WithFields(logrus.Fields{
			"port":       port,
			"leaderPort": leaderPort,
		}).WithError(err).Error("Failed to send removeNode request to leader")
		return fmt.Errorf("failed to send removeNode request to leader: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		nm.logger.WithFields(logrus.Fields{
			"port":       port,
			"leaderPort": leaderPort,
			"response":   string(body),
		}).Error("Leader failed to remove node")
		return fmt.Errorf("leader failed to remove node: %s", string(body))
	}

	// Kill the process running on the specified port
	if err := nm.killProcessOnPort(port); err != nil {
		nm.logger.WithFields(logrus.Fields{
			"port": port,
		}).WithError(err).Error("Failed to kill process on port")
		return fmt.Errorf("failed to kill process on port %d: %v", port, err)
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

// killProcessOnPort finds and kills the process running on the specified port.
func (nm *NodeManager) killProcessOnPort(port int) error {
	// Use lsof to find the process ID (PID) of the application running on the port
	cmd := exec.Command("lsof", "-t", fmt.Sprintf("-i:%d", port))
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to find process on port %d: %v", port, err)
	}

	// Parse the PID from the output
	pid := string(output)
	if pid == "" {
		return fmt.Errorf("no process found on port %d", port)
	}

	// Kill the process using the PID
	killCmd := exec.Command("kill", "-9", pid)
	if err := killCmd.Run(); err != nil {
		return fmt.Errorf("failed to kill process with PID %s: %v", pid, err)
	}

	nm.logger.WithFields(logrus.Fields{
		"port": port,
		"pid":  pid,
	}).Info("Process killed successfully")
	return nil
}

// GetLeader reads the leader's port from the leaderInfo.txt file.
func (nm *NodeManager) GetLeader() (int, error) {
	leaderInfoPath := filepath.Join(nm.baseDataDir, "leaderInfo.txt")
	data, err := os.ReadFile(leaderInfoPath)
	if err != nil {
		nm.logger.WithError(err).Error("Failed to read leaderInfo.txt")
		return 0, fmt.Errorf("failed to read leaderInfo.txt: %v", err)
	}

	leaderPort, err := strconv.Atoi(string(data))
	if err != nil {
		nm.logger.WithError(err).Error("Invalid leader port in leaderInfo.txt")
		return 0, fmt.Errorf("invalid leader port in leaderInfo.txt: %v", err)
	}

	nm.logger.WithFields(logrus.Fields{
		"leaderPort": leaderPort,
	}).Info("Leader port retrieved successfully")
	return leaderPort, nil
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
