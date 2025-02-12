package websocket

import (
	"log"
	"net/http"
	"sync"

	"cryptobot_server/kafka"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// WebSocket handler
func WSHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading connection:", err)
		return
	}
	defer conn.Close()

	// Create a channel for sending WebSocket messages
	messageChannel := make(chan []byte)

	// Start a goroutine for writing to the WebSocket
	go writeToWebSocket(conn, messageChannel)

	// Setup Kafka consumer to subscribe to multiple topics concurrently
	var wg sync.WaitGroup
	topics := []string{"orderbook", "pnl", "wallet", "trade", "trade_dictionary"}

	for _, topic := range topics {
		wg.Add(1)
		go kafka.ConsumeMessages(topic, messageChannel, &wg)
	}

	// Wait for all consumers to finish
	wg.Wait()
}

// Function to handle WebSocket writes
func writeToWebSocket(conn *websocket.Conn, messageChannel chan []byte) {
	for message := range messageChannel {
		// Write the message to WebSocket connection
		err := conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Println("Error sending message over WebSocket:", err)
			return
		}
	}
}
