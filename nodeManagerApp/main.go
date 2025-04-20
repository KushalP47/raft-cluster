package main

import (
	"log"
	"net/http"

	"github.com/KushalP47/raft-cluster/nodeManagerApp/handlers"
	"github.com/KushalP47/raft-cluster/nodeManagerApp/manager"
)

func main() {
	// Initialize NodeManager
	nodeManager := manager.NewNodeManager()

	// Register API handlers
	http.HandleFunc("/addNode", handlers.AddNodeHandler(nodeManager))
	http.HandleFunc("/stopNode", handlers.StopNodeHandler(nodeManager))
	http.HandleFunc("/clientRequest", handlers.ClientRequestHandler(nodeManager))

	// Start the HTTP server
	log.Println("Starting NodeManager application on port 8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
