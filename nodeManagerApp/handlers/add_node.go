package handlers

import (
    "fmt"
    "net/http"
    "strconv"

    "github.com/KushalP47/raft-cluster/nodeManagerApp/manager"
)

func AddNodeHandler(nodeManager *manager.NodeManager) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Parse the port from the query parameter
        portStr := r.URL.Query().Get("port")
        port, err := strconv.Atoi(portStr)
        if err != nil {
            http.Error(w, "Invalid port", http.StatusBadRequest)
            return
        }

        // Check if the port is already in use
        nodeState, err := nodeManager.GetNodeState(port)
        if err == nil && nodeState.Status {
            http.Error(w, fmt.Sprintf("Node on port %d is already running", port), http.StatusConflict)
            return
        }

        // Determine if the node should bootstrap or join the cluster
        leader, err := nodeManager.GetLeader()
        var leaderAddr string
        if err != nil {
            // No leader exists, bootstrap the cluster
            leaderAddr = ""
        } else {
            // Leader exists, join the cluster
            leaderAddr = fmt.Sprintf("127.0.0.1:%d", leader)
        }

        // Add the node to the cluster using the NodeManager
        if err := nodeManager.AddNode(port, leaderAddr == ""); err != nil {
            http.Error(w, fmt.Sprintf("Failed to add node: %v", err), http.StatusInternalServerError)
            return
        }

        // Respond with success
        fmt.Fprintf(w, "Node started on port %d", port)
    }
}