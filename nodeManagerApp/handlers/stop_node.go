package handlers

import (
    "fmt"
    "net/http"
    "strconv"

    "github.com/KushalP47/raft-cluster/nodeManagerApp/manager"
)

func StopNodeHandler(nodeManager *manager.NodeManager) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Parse the port from the query parameter
        portStr := r.URL.Query().Get("port")
        port, err := strconv.Atoi(portStr)
        if err != nil {
            http.Error(w, "Invalid port", http.StatusBadRequest)
            return
        }

        // Check if the node is running
        nodeState, err := nodeManager.GetNodeState(port)
        if err != nil || !nodeState.Status {
            http.Error(w, fmt.Sprintf("No running node found on port %d", port), http.StatusNotFound)
            return
        }

        // Remove the node using the NodeManager
        if err := nodeManager.RemoveNode(port); err != nil {
            http.Error(w, fmt.Sprintf("Failed to stop node: %v", err), http.StatusInternalServerError)
            return
        }

        // Respond with success
        fmt.Fprintf(w, "Node stopped on port %d", port)
    }
}