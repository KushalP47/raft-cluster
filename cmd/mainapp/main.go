package main

import (
    "net/http"
    "raft-cluster/internal/fsm"
)

func main() {
    // Initialize node manager
    nodeManager := NewNodeManager()
    
    // Start API server
    http.HandleFunc("/addNode", addNodeHandler)
    http.HandleFunc("/stopNode", stopNodeHandler)
    http.HandleFunc("/clientRequest", clientRequestHandler)
    http.ListenAndServe(":8000", nil)
}

