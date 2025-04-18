package handlers

import (
    "fmt"
    "io"
    "net/http"

    "github.com/KushalP47/raft-cluster/nodeManagerApp/manager"
)

func ClientRequestHandler(nodeManager *manager.NodeManager) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Get the leader's port
        leaderPort, err := nodeManager.GetLeader()
        if err != nil {
            http.Error(w, "No leader available", http.StatusServiceUnavailable)
            return
        }

        // Forward the request to the leader node
        leaderURL := fmt.Sprintf("http://localhost:%d/execute", leaderPort)
        req, err := http.NewRequest(r.Method, leaderURL, r.Body)
        if err != nil {
            http.Error(w, fmt.Sprintf("Failed to create request: %v", err), http.StatusInternalServerError)
            return
        }

        // Copy headers from the original request
        for key, values := range r.Header {
            for _, value := range values {
                req.Header.Add(key, value)
            }
        }

        // Send the request to the leader
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            http.Error(w, fmt.Sprintf("Failed to forward request to leader: %v", err), http.StatusInternalServerError)
            return
        }
        defer resp.Body.Close()

        // Read the response from the leader
        body, err := io.ReadAll(resp.Body)
        if err != nil {
            http.Error(w, fmt.Sprintf("Failed to read response from leader: %v", err), http.StatusInternalServerError)
            return
        }

        // Respond to the client based on the leader's response
        if resp.StatusCode == http.StatusOK {
            w.WriteHeader(http.StatusOK)
            fmt.Fprint(w, "Request successfully processed by leader: ", string(body))
        } else {
            w.WriteHeader(http.StatusInternalServerError)
            fmt.Fprint(w, "Leader failed to process request: ", string(body))
        }
    }
}